/*
 *    The contents of this file are subject to the Initial
 *    Developer's Public License Version 1.0 (the "License");
 *    you may not use this file except in compliance with the
 *    License. You may obtain a copy of the License at
 *    https://github.com/FirebirdSQL/NETProvider/raw/master/license.txt.
 *
 *    All Rights Reserved.
 */

using System;
using System.Collections;
using System.IO;
using System.Reflection;
using System.Security.Cryptography;
using System.Threading.Tasks;
using FirebirdSql.Data.TestsBase;
using NUnit.Framework;

namespace FirebirdSql.Data.FirebirdClient.Tests;

[TestFixtureSource(typeof(FbServerTypeTestFixtureSource), nameof(FbServerTypeTestFixtureSource.Default))]
public class FbInlineBlobTests : FbTestsBase
{
	public FbInlineBlobTests(FbServerType serverType, bool compression, FbWireCrypt wireCrypt)
		: base(serverType, compression, wireCrypt)
	{ }

	[Test]
	public void DecodeSegments()
	{
		var data = new byte[] { 2, 0, 0xAA, 0xBB, 0, 0, 1, 0, 0xCC };
		CollectionAssert.AreEqual(new byte[] { 0xAA, 0xBB, 0xCC }, Client.Managed.Version10.GdsBlob.DecodeSegments(data));
		CollectionAssert.IsEmpty(Client.Managed.Version10.GdsBlob.DecodeSegments(Array.Empty<byte>()));
	}

	[TestCase(0)]
	[TestCase(1)]
	[TestCase(1000)]
	[TestCase(60000)]
	[TestCase(70000)] // above the inline limit, read from the server
	public async Task BinaryBlobRoundTrip(int size)
	{
		if (!EnsureInlineBlobsSupported())
			return;

		var values = new[] { RandomNumberGenerator.GetBytes(size), RandomNumberGenerator.GetBytes(size) };
		var ids = await InsertBlobs(values);

		await using (var select = new FbCommand($"SELECT blob_field FROM TEST WHERE int_field IN ({ids[0]}, {ids[1]}) ORDER BY int_field", Connection))
		{
			await using (var reader = await select.ExecuteReaderAsync())
			{
				var expected = ids[0] < ids[1] ? values : new[] { values[1], values[0] };
				Assert.IsTrue(await reader.ReadAsync());
				CollectionAssert.AreEqual(expected[0], (byte[])reader[0]);
				Assert.IsTrue(await reader.ReadAsync());
				CollectionAssert.AreEqual(expected[1], (byte[])reader[0]);
				Assert.IsFalse(await reader.ReadAsync());
			}
		}
	}

	[Test]
	public async Task TextBlobIsReceivedInline()
	{
		if (!EnsureInlineBlobsSupported())
			return;

		await using (var transaction = await Connection.BeginTransactionAsync())
		{
			await using (var select = new FbCommand("SELECT CAST('inline text' AS BLOB SUB_TYPE TEXT) FROM RDB$DATABASE", Connection, transaction))
			{
				await using (var reader = await select.ExecuteReaderAsync())
				{
					Assert.IsTrue(await reader.ReadAsync());
					Assert.AreEqual(1, InlineBlobCacheCount(), "blob was not sent inline");
					Assert.AreEqual("inline text", reader.GetString(0));
					Assert.AreEqual(0, InlineBlobCacheCount(), "inline blob was not taken from the cache");
				}
			}
			await transaction.CommitAsync();
		}
	}

	[Test]
	public async Task UnreadInlineBlobsAreReleasedOnCommit()
	{
		if (!EnsureInlineBlobsSupported())
			return;

		await using (var transaction = await Connection.BeginTransactionAsync())
		{
			await using (var select = new FbCommand("SELECT CAST('x' AS BLOB SUB_TYPE TEXT) FROM RDB$DATABASE", Connection, transaction))
			{
				await using (var reader = await select.ExecuteReaderAsync())
				{
					Assert.IsTrue(await reader.ReadAsync());
				}
			}
			Assert.AreEqual(1, InlineBlobCacheCount());
			await transaction.CommitAsync();
		}
		Assert.AreEqual(0, InlineBlobCacheCount());
	}

	[Test]
	public async Task ExecuteProcedureBlobOutput()
	{
		if (!EnsureInlineBlobsSupported())
			return;

		var id = RandomNumberGenerator.GetInt32(int.MinValue, int.MaxValue);
		await using (var insert = new FbCommand("INSERT INTO TEST (int_field, clob_field) values(@id, 'from procedure')", Connection))
		{
			insert.Parameters.Add("@id", FbDbType.Integer).Value = id;
			await insert.ExecuteNonQueryAsync();
		}

		// EXECUTE PROCEDURE uses op_execute2, inline blobs arrive before op_sql_response
		await using (var cmd = new FbCommand("EXECUTE PROCEDURE GETASCIIBLOB(@id)", Connection))
		{
			cmd.Parameters.Add("@id", FbDbType.Integer).Value = id;
			Assert.AreEqual("from procedure", await cmd.ExecuteScalarAsync());
		}
	}

	[Test]
	public async Task BlobStreamSeekOnInlineBlob()
	{
		if (!EnsureInlineBlobsSupported())
			return;

		var value = RandomNumberGenerator.GetBytes(1000);
		var ids = await InsertBlobs(new[] { value });

		await using (var select = new FbCommand($"SELECT blob_field FROM TEST WHERE int_field = {ids[0]}", Connection))
		{
			await using (var reader = await select.ExecuteReaderAsync())
			{
				Assert.IsTrue(await reader.ReadAsync());
				await using (var stream = reader.GetStream(0))
				{
					Assert.AreEqual(value.Length, stream.Length);
					var buffer = new byte[value.Length];
					var read = 0;
					while (read < buffer.Length)
						read += await stream.ReadAsync(buffer, read, buffer.Length - read);
					CollectionAssert.AreEqual(value, buffer);
				}
			}
		}
	}

	[Test]
	public async Task DisabledInlineBlobs()
	{
		if (!EnsureInlineBlobsSupported())
			return;

		var csb = BuildConnectionStringBuilder(ServerType, Compression, WireCrypt);
		csb.MaxInlineBlobSize = 0;
		await using (var connection = new FbConnection(csb.ToString()))
		{
			await connection.OpenAsync();
			await using (var select = new FbCommand("SELECT CAST('not inline' AS BLOB SUB_TYPE TEXT) FROM RDB$DATABASE", connection))
			{
				await using (var reader = await select.ExecuteReaderAsync())
				{
					Assert.IsTrue(await reader.ReadAsync());
					Assert.AreEqual(0, InlineBlobCacheCount(connection));
					Assert.AreEqual("not inline", reader.GetString(0));
				}
			}
		}
	}

	bool EnsureInlineBlobsSupported()
	{
		if (Connection.InnerConnection.Database is Client.Managed.Version19.GdsDatabase)
			return true;
		Assert.Inconclusive("Server does not support protocol 19 (inline blobs).");
		return false;
	}

	int InlineBlobCacheCount(FbConnection connection = null)
	{
		var database = (connection ?? Connection).InnerConnection.Database;
		var field = typeof(Client.Managed.Version19.GdsDatabase).GetField("_inlineBlobs", BindingFlags.NonPublic | BindingFlags.Instance);
		return ((ICollection)field.GetValue(database)).Count;
	}

	async Task<int[]> InsertBlobs(byte[][] values)
	{
		var ids = new int[values.Length];
		await using (var transaction = await Connection.BeginTransactionAsync())
		{
			for (var i = 0; i < values.Length; i++)
			{
				ids[i] = RandomNumberGenerator.GetInt32(int.MinValue, int.MaxValue);
				await using (var insert = new FbCommand("INSERT INTO TEST (int_field, blob_field) values(@int_field, @blob_field)", Connection, transaction))
				{
					insert.Parameters.Add("@int_field", FbDbType.Integer).Value = ids[i];
					insert.Parameters.Add("@blob_field", FbDbType.Binary).Value = values[i];
					await insert.ExecuteNonQueryAsync();
				}
			}
			await transaction.CommitAsync();
		}
		return ids;
	}
}
