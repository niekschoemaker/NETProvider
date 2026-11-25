/*
 *    The contents of this file are subject to the Initial
 *    Developer's Public License Version 1.0 (the "License");
 *    you may not use this file except in compliance with the
 *    License. You may obtain a copy of the License at
 *    https://github.com/FirebirdSQL/NETProvider/raw/master/license.txt.
 *
 *    Software distributed under the License is distributed on
 *    an "AS IS" basis, WITHOUT WARRANTY OF ANY KIND, either
 *    express or implied. See the License for the specific
 *    language governing rights and limitations under the License.
 *
 *    All Rights Reserved.
 */

using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FirebirdSql.Data.Common;

namespace FirebirdSql.Data.Client.Managed.Version19;

internal class GdsDatabase : Version18.GdsDatabase
{
	// Same default as the Firebird client (DEFAULT_BLOBS_CACHE_SIZE).
	const int MaxInlineBlobCacheSize = 10 * 1024 * 1024;

	private readonly Dictionary<(int TransactionHandle, long BlobId), byte[]> _inlineBlobs = new();
	private int _inlineBlobsSize;

	public int MaxInlineBlobSize { get; }

	public GdsDatabase(GdsConnection connection, int maxInlineBlobSize)
		: base(connection)
	{
		MaxInlineBlobSize = maxInlineBlobSize;
	}

	public override StatementBase CreateStatement()
	{
		return new GdsStatement(this);
	}

	public override StatementBase CreateStatement(TransactionBase transaction)
	{
		return new GdsStatement(this, (Version10.GdsTransaction)transaction);
	}

	public override int ReadOperation()
	{
		var operation = base.ReadOperation();
		while (operation == IscCodes.op_inline_blob)
		{
			var transactionHandle = Xdr.ReadInt32();
			var blobId = Xdr.ReadInt64();
			Xdr.ReadBuffer(); // blob info
			var data = Xdr.ReadOpaque(Xdr.ReadInt32());
			AddInlineBlob(transactionHandle, blobId, data);
			operation = base.ReadOperation();
		}
		return operation;
	}
	public override async ValueTask<int> ReadOperationAsync(CancellationToken cancellationToken = default)
	{
		var operation = await base.ReadOperationAsync(cancellationToken).ConfigureAwait(false);
		while (operation == IscCodes.op_inline_blob)
		{
			var transactionHandle = await Xdr.ReadInt32Async(cancellationToken).ConfigureAwait(false);
			var blobId = await Xdr.ReadInt64Async(cancellationToken).ConfigureAwait(false);
			await Xdr.ReadBufferAsync(cancellationToken).ConfigureAwait(false); // blob info
			var data = await Xdr.ReadOpaqueAsync(await Xdr.ReadInt32Async(cancellationToken).ConfigureAwait(false), cancellationToken).ConfigureAwait(false);
			AddInlineBlob(transactionHandle, blobId, data);
			operation = await base.ReadOperationAsync(cancellationToken).ConfigureAwait(false);
		}
		return operation;
	}

	public override bool TryTakeInlineBlob(int transactionHandle, long blobId, out byte[] data)
	{
		if (!_inlineBlobs.Remove((transactionHandle, blobId), out data))
			return false;
		_inlineBlobsSize -= data.Length;
		return true;
	}

	public override void ReleaseInlineBlobs(int transactionHandle)
	{
		foreach (var key in _inlineBlobs.Keys.Where(x => x.TransactionHandle == transactionHandle).ToList())
		{
			TryTakeInlineBlob(key.TransactionHandle, key.BlobId, out _);
		}
	}

	private void AddInlineBlob(int transactionHandle, long blobId, byte[] data)
	{
		TryTakeInlineBlob(transactionHandle, blobId, out _);
		// Over the limit the blob is simply not cached and will be fetched from the server when read.
		if (_inlineBlobsSize + data.Length > MaxInlineBlobCacheSize)
			return;
		_inlineBlobs[(transactionHandle, blobId)] = data;
		_inlineBlobsSize += data.Length;
	}
}
