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

//$Authors = Carlos Guzman Alvarez, Jiri Cincura (jiri@cincura.net)

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using FirebirdSql.Data.Common;

namespace FirebirdSql.Data.Client.Managed.Version10;

internal sealed class GdsBlob : BlobBase
{
	const int DataSegment = 0;
	const int SeekMode = 0;

	#region Fields

	private readonly GdsDatabase _database;
	private int _blobHandle;
	// Blob content received inline (protocol 19+); when set, no server-side blob is open.
	private byte[] _inlineContent;

	#endregion

	#region Properties

	public override DatabaseBase Database
	{
		get { return _database; }
	}

	public override int Handle
	{
		get { return _blobHandle; }
	}

	#endregion

	#region Constructors

	public GdsBlob(GdsDatabase database, GdsTransaction transaction)
		: this(database, transaction, 0)
	{ }

	public GdsBlob(GdsDatabase database, GdsTransaction transaction, long blobId)
		: base(database)
	{
		_database = database;
		_transaction = transaction;
		_position = 0;
		_blobHandle = 0;
		_blobId = blobId;
	}

	#endregion

	#region Protected Methods

	public override void Create()
	{
		try
		{
			CreateOrOpen(IscCodes.op_create_blob, null);
			RblAddValue(IscCodes.RBL_create);
		}
		catch (IscException)
		{
			throw;
		}
	}
	public override async ValueTask CreateAsync(CancellationToken cancellationToken = default)
	{
		try
		{
			await CreateOrOpenAsync(IscCodes.op_create_blob, null, cancellationToken).ConfigureAwait(false);
			RblAddValue(IscCodes.RBL_create);
		}
		catch (IscException)
		{
			throw;
		}
	}

	public override void Open()
	{
		if (TryOpenInline())
			return;

		try
		{
			CreateOrOpen(IscCodes.op_open_blob, null);
		}
		catch (IscException)
		{
			throw;
		}
	}
	public override async ValueTask OpenAsync(CancellationToken cancellationToken = default)
	{
		if (TryOpenInline())
			return;

		try
		{
			await CreateOrOpenAsync(IscCodes.op_open_blob, null, cancellationToken).ConfigureAwait(false);
		}
		catch (IscException)
		{
			throw;
		}
	}

	public override int GetLength()
	{
		try
		{
			if (!IsOpen)
				Open();

			if (_inlineContent != null)
				return _inlineContent.Length;

			var bufferLength = 20;
			var buffer = new byte[bufferLength];

			_database.Xdr.Write(IscCodes.op_info_blob);
			_database.Xdr.Write(_blobHandle);
			_database.Xdr.Write(0);
			_database.Xdr.WriteBuffer(new byte[] { IscCodes.isc_info_blob_total_length }, 1);
			_database.Xdr.Write(bufferLength);

			_database.Xdr.Flush();

			var response = (GenericResponse)_database.ReadResponse();

			var responseLength = bufferLength;

			if (response.Data.Length < bufferLength)
			{
				responseLength = response.Data.Length;
			}

			Buffer.BlockCopy(response.Data, 0, buffer, 0, responseLength);

			var length = IscHelper.VaxInteger(buffer, 1, 2);
			var size = IscHelper.VaxInteger(buffer, 3, (int)length);

			return (int)size;
		}
		catch (IOException ex)
		{
			throw IscException.ForIOException(ex);
		}
	}

	public override async ValueTask<int> GetLengthAsync(CancellationToken cancellationToken = default)
	{
		try
		{
			if (!IsOpen)
				await OpenAsync(cancellationToken).ConfigureAwait(false);

			if (_inlineContent != null)
				return _inlineContent.Length;

			var bufferLength = 20;
			var buffer = new byte[bufferLength];

			await _database.Xdr.WriteAsync(IscCodes.op_info_blob, cancellationToken).ConfigureAwait(false);
			await _database.Xdr.WriteAsync(_blobHandle, cancellationToken).ConfigureAwait(false);
			await _database.Xdr.WriteAsync(0, cancellationToken).ConfigureAwait(false);
			await _database.Xdr.WriteBufferAsync(new byte[] { IscCodes.isc_info_blob_total_length }, 1, cancellationToken).ConfigureAwait(false);
			await _database.Xdr.WriteAsync(bufferLength, cancellationToken).ConfigureAwait(false);

			await _database.Xdr.FlushAsync(cancellationToken).ConfigureAwait(false);

			var response = (GenericResponse)await _database.ReadResponseAsync(cancellationToken).ConfigureAwait(false);

			var responseLength = bufferLength;

			if (response.Data.Length < bufferLength)
			{
				responseLength = response.Data.Length;
			}

			Buffer.BlockCopy(response.Data, 0, buffer, 0, responseLength);

			var length = IscHelper.VaxInteger(buffer, 1, 2);
			var size = IscHelper.VaxInteger(buffer, 3, (int)length);

			return (int)size;
		}
		catch (IOException ex)
		{
			throw IscException.ForIOException(ex);
		}
	}

	public override void GetSegment(Stream stream)
	{
		if (_inlineContent != null)
		{
			stream.Write(TakeInlineContent());
			return;
		}

		var requested = SegmentSize;

		try
		{
			_database.Xdr.Write(IscCodes.op_get_segment);
			_database.Xdr.Write(_blobHandle);
			_database.Xdr.Write(requested < short.MaxValue - 12 ? requested : short.MaxValue - 12);
			_database.Xdr.Write(DataSegment);
			_database.Xdr.Flush();

			var response = (GenericResponse)_database.ReadResponse();

			RblRemoveValue(IscCodes.RBL_segment);
			if (response.ObjectHandle == 1)
			{
				RblAddValue(IscCodes.RBL_segment);
			}
			else if (response.ObjectHandle == 2)
			{
				RblAddValue(IscCodes.RBL_eof_pending);
			}

			var buffer = response.Data;

			if (buffer.Length == 0)
			{
				// previous segment was last, this has no data
				return;
			}

			var len = 0;
			var srcpos = 0;

			while (srcpos < buffer.Length)
			{
				len = (int)IscHelper.VaxInteger(buffer, srcpos, 2);
				srcpos += 2;

				stream.Write(buffer, srcpos, len);
				srcpos += len;
			}
		}
		catch (IOException ex)
		{
			throw IscException.ForIOException(ex);
		}
	}
	public override async ValueTask GetSegmentAsync(Stream stream, CancellationToken cancellationToken = default)
	{
		if (_inlineContent != null)
		{
			await stream.WriteAsync(TakeInlineContent(), cancellationToken).ConfigureAwait(false);
			return;
		}

		var requested = SegmentSize;

		try
		{
			await _database.Xdr.WriteAsync(IscCodes.op_get_segment, cancellationToken).ConfigureAwait(false);
			await _database.Xdr.WriteAsync(_blobHandle, cancellationToken).ConfigureAwait(false);
			await _database.Xdr.WriteAsync(requested < short.MaxValue - 12 ? requested : short.MaxValue - 12, cancellationToken).ConfigureAwait(false);
			await _database.Xdr.WriteAsync(DataSegment, cancellationToken).ConfigureAwait(false);
			await _database.Xdr.FlushAsync(cancellationToken).ConfigureAwait(false);

			var response = (GenericResponse)await _database.ReadResponseAsync(cancellationToken).ConfigureAwait(false);

			RblRemoveValue(IscCodes.RBL_segment);
			if (response.ObjectHandle == 1)
			{
				RblAddValue(IscCodes.RBL_segment);
			}
			else if (response.ObjectHandle == 2)
			{
				RblAddValue(IscCodes.RBL_eof_pending);
			}

			var buffer = response.Data;

			if (buffer.Length == 0)
			{
				//previous segment was last, this has no data
				return;
			}

			var len = 0;
			var srcpos = 0;

			while (srcpos < buffer.Length)
			{
				len = (int)IscHelper.VaxInteger(buffer, srcpos, 2);
				srcpos += 2;

				stream.Write(buffer, srcpos, len);
				srcpos += len;
			}
		}
		catch (IOException ex)
		{
			throw IscException.ForIOException(ex);
		}
	}

	public override byte[] GetSegment()
	{
		if (_inlineContent != null)
			return TakeInlineContent();

		var requested = SegmentSize;

		try
		{
			_database.Xdr.Write(IscCodes.op_get_segment);
			_database.Xdr.Write(_blobHandle);
			_database.Xdr.Write(requested < short.MaxValue - 12 ? requested : short.MaxValue - 12);
			_database.Xdr.Write(DataSegment);
			_database.Xdr.Flush();

			var response = (GenericResponse)_database.ReadResponse();

			RblRemoveValue(IscCodes.RBL_segment);
			if (response.ObjectHandle == 1)
			{
				RblAddValue(IscCodes.RBL_segment);
			}
			else if (response.ObjectHandle == 2)
			{
				RblAddValue(IscCodes.RBL_eof_pending);
			}

			var buffer = response.Data;

			if (buffer.Length == 0)
			{
				//previous segment was last, this has no data
				return Array.Empty<byte>();
			}

			var posInInput = 0;
			var posInOutput = 0;

			var tmp = new byte[requested * 2];
			while (posInInput < buffer.Length)
			{
				var len = (int)IscHelper.VaxInteger(buffer, posInInput, 2);
				posInInput += 2;

				Array.Copy(buffer, posInInput, tmp, posInOutput, len);
				posInOutput += len;
				posInInput += len;
			}

			var actualBuffer = new byte[posInOutput];
			Array.Copy(tmp, actualBuffer, posInOutput);

			return actualBuffer;
		}
		catch (IOException ex)
		{
			throw IscException.ForIOException(ex);
		}
	}
	public override async ValueTask<byte[]> GetSegmentAsync(CancellationToken cancellationToken = default)
	{
		if (_inlineContent != null)
			return TakeInlineContent();

		var requested = SegmentSize;

		try
		{
			await _database.Xdr.WriteAsync(IscCodes.op_get_segment, cancellationToken).ConfigureAwait(false);
			await _database.Xdr.WriteAsync(_blobHandle, cancellationToken).ConfigureAwait(false);
			await _database.Xdr.WriteAsync(requested < short.MaxValue - 12 ? requested : short.MaxValue - 12, cancellationToken).ConfigureAwait(false);
			await _database.Xdr.WriteAsync(DataSegment, cancellationToken).ConfigureAwait(false);
			await _database.Xdr.FlushAsync(cancellationToken).ConfigureAwait(false);

			var response = (GenericResponse)await _database.ReadResponseAsync(cancellationToken).ConfigureAwait(false);

			RblRemoveValue(IscCodes.RBL_segment);
			if (response.ObjectHandle == 1)
			{
				RblAddValue(IscCodes.RBL_segment);
			}
			else if (response.ObjectHandle == 2)
			{
				RblAddValue(IscCodes.RBL_eof_pending);
			}

			var buffer = response.Data;

			if (buffer.Length == 0)
			{
				// previous segment was last, this has no data
				return Array.Empty<byte>();
			}

			var posInInput = 0;
			var posInOutput = 0;

			var tmp = new byte[requested * 2];
			while (posInInput < buffer.Length)
			{
				var len = (int)IscHelper.VaxInteger(buffer, posInInput, 2);
				posInInput += 2;

				Array.Copy(buffer, posInInput, tmp, posInOutput, len);
				posInOutput += len;
				posInInput += len;
			}

			var actualBuffer = new byte[posInOutput];
			Array.Copy(tmp, actualBuffer, posInOutput);

			return actualBuffer;
		}
		catch (IOException ex)
		{
			throw IscException.ForIOException(ex);
		}
	}

	public override void PutSegment(byte[] buffer)
	{
		try
		{
			_database.Xdr.Write(IscCodes.op_batch_segments);
			_database.Xdr.Write(_blobHandle);
			_database.Xdr.WriteBlobBuffer(buffer);
			_database.Xdr.Flush();

			_database.ReadResponse();
		}
		catch (IOException ex)
		{
			throw IscException.ForIOException(ex);
		}
	}
	public override async ValueTask PutSegmentAsync(byte[] buffer, CancellationToken cancellationToken = default)
	{
		try
		{
			await _database.Xdr.WriteAsync(IscCodes.op_batch_segments, cancellationToken).ConfigureAwait(false);
			await _database.Xdr.WriteAsync(_blobHandle, cancellationToken).ConfigureAwait(false);
			await _database.Xdr.WriteBlobBufferAsync(buffer, cancellationToken).ConfigureAwait(false);
			await _database.Xdr.FlushAsync(cancellationToken).ConfigureAwait(false);

			await _database.ReadResponseAsync(cancellationToken).ConfigureAwait(false);
		}
		catch (IOException ex)
		{
			throw IscException.ForIOException(ex);
		}
	}

	public override void Seek(int offset, int seekMode)
	{
		if (_inlineContent != null)
		{
			// seeking is done by the server, so switch to a regular server-side blob
			_inlineContent = null;
			RblRemoveValue(IscCodes.RBL_eof_pending);
			CreateOrOpen(IscCodes.op_open_blob, null);
		}

		try
		{
			_database.Xdr.Write(IscCodes.op_seek_blob);
			_database.Xdr.Write(_blobHandle);
			_database.Xdr.Write(seekMode);
			_database.Xdr.Write(offset);
			_database.Xdr.Flush();

			var response = (GenericResponse)_database.ReadResponse();

			_position = response.ObjectHandle;
		}
		catch (IOException ex)
		{
			throw IscException.ForIOException(ex);
		}
	}
	public override async ValueTask SeekAsync(int offset, int seekMode, CancellationToken cancellationToken = default)
	{
		if (_inlineContent != null)
		{
			// seeking is done by the server, so switch to a regular server-side blob
			_inlineContent = null;
			RblRemoveValue(IscCodes.RBL_eof_pending);
			await CreateOrOpenAsync(IscCodes.op_open_blob, null, cancellationToken).ConfigureAwait(false);
		}

		try
		{
			await _database.Xdr.WriteAsync(IscCodes.op_seek_blob, cancellationToken).ConfigureAwait(false);
			await _database.Xdr.WriteAsync(_blobHandle, cancellationToken).ConfigureAwait(false);
			await _database.Xdr.WriteAsync(seekMode, cancellationToken).ConfigureAwait(false);
			await _database.Xdr.WriteAsync(offset, cancellationToken).ConfigureAwait(false);
			await _database.Xdr.FlushAsync(cancellationToken).ConfigureAwait(false);

			var response = (GenericResponse)await _database.ReadResponseAsync(cancellationToken).ConfigureAwait(false);

			_position = response.ObjectHandle;
		}
		catch (IOException ex)
		{
			throw IscException.ForIOException(ex);
		}
	}

	public override void Close()
	{
		if (_inlineContent != null)
			return;
		_database.ReleaseObject(IscCodes.op_close_blob, _blobHandle);
	}
	public override ValueTask CloseAsync(CancellationToken cancellationToken = default)
	{
		if (_inlineContent != null)
			return ValueTask.CompletedTask;
		return _database.ReleaseObjectAsync(IscCodes.op_close_blob, _blobHandle, cancellationToken);
	}

	public override void Cancel()
	{
		if (_inlineContent != null)
			return;
		_database.ReleaseObject(IscCodes.op_cancel_blob, _blobHandle);
	}
	public override ValueTask CancelAsync(CancellationToken cancellationToken = default)
	{
		if (_inlineContent != null)
			return ValueTask.CompletedTask;
		return _database.ReleaseObjectAsync(IscCodes.op_cancel_blob, _blobHandle, cancellationToken);
	}

	#endregion

	#region Private API Methods

	private bool TryOpenInline()
	{
		if (_transaction == null || !_database.TryTakeInlineBlob(_transaction.Handle, _blobId, out var data))
			return false;
		_inlineContent = DecodeSegments(data);
		_isOpen = true;
		return true;
	}

	private byte[] TakeInlineContent()
	{
		if (EOF)
			return Array.Empty<byte>();
		RblAddValue(IscCodes.RBL_eof_pending);
		return _inlineContent;
	}

	// Inline blob data is a sequence of segments, each prefixed by its 2-byte little-endian length.
	internal static byte[] DecodeSegments(byte[] data)
	{
		var length = 0;
		for (var pos = 0; pos < data.Length; pos += 2 + data[pos] + (data[pos + 1] << 8))
		{
			length += data[pos] + (data[pos + 1] << 8);
		}
		var result = new byte[length];
		var offset = 0;
		for (var pos = 0; pos < data.Length;)
		{
			var segmentLength = data[pos] + (data[pos + 1] << 8);
			Buffer.BlockCopy(data, pos + 2, result, offset, segmentLength);
			offset += segmentLength;
			pos += 2 + segmentLength;
		}
		return result;
	}

	private void CreateOrOpen(int op, BlobParameterBuffer bpb)
	{
		try
		{
			_database.Xdr.Write(op);
			if (bpb != null)
			{
				_database.Xdr.WriteTyped(IscCodes.isc_bpb_version1, bpb.ToArray());
			}
			_database.Xdr.Write(_transaction.Handle);
			_database.Xdr.Write(_blobId);
			_database.Xdr.Flush();

			var response = (GenericResponse)_database.ReadResponse();

			_blobId = response.BlobId;
			_blobHandle = response.ObjectHandle;
			_isOpen = true;
		}
		catch (IOException ex)
		{
			throw IscException.ForIOException(ex);
		}
	}
	private async ValueTask CreateOrOpenAsync(int op, BlobParameterBuffer bpb, CancellationToken cancellationToken = default)
	{
		try
		{
			await _database.Xdr.WriteAsync(op, cancellationToken).ConfigureAwait(false);
			if (bpb != null)
			{
				await _database.Xdr.WriteTypedAsync(IscCodes.isc_bpb_version1, bpb.ToArray(), cancellationToken).ConfigureAwait(false);
			}
			await _database.Xdr.WriteAsync(_transaction.Handle, cancellationToken).ConfigureAwait(false);
			await _database.Xdr.WriteAsync(_blobId, cancellationToken).ConfigureAwait(false);
			await _database.Xdr.FlushAsync(cancellationToken).ConfigureAwait(false);

			var response = (GenericResponse)await _database.ReadResponseAsync(cancellationToken).ConfigureAwait(false);

			_blobId = response.BlobId;
			_blobHandle = response.ObjectHandle;
			_isOpen = true;
		}
		catch (IOException ex)
		{
			throw IscException.ForIOException(ex);
		}
	}

	#endregion
}
