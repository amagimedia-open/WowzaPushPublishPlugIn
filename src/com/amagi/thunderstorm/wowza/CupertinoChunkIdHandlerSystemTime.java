package com.amagi.thunderstorm.wowza;

import com.wowza.wms.httpstreamer.cupertinostreaming.livestreampacketizer.HTTPStreamerCupertinoLiveStreamPacketizerChunkIdContext;
import com.wowza.wms.httpstreamer.cupertinostreaming.livestreampacketizer.IHTTPStreamerCupertinoLiveStreamPacketizerChunkIdHandler;
import com.wowza.wms.httpstreamer.cupertinostreaming.livestreampacketizer.LiveStreamPacketizerCupertino;

public class CupertinoChunkIdHandlerSystemTime implements IHTTPStreamerCupertinoLiveStreamPacketizerChunkIdHandler {

	 long offset = -1;
	 
	 @Override
	 public void init(LiveStreamPacketizerCupertino liveStreamPacketizer)
	 {
		 
	  offset = System.currentTimeMillis() / liveStreamPacketizer.getChunkDurationTarget();
	  
	 }

	 @Override
	 public long onAssignChunkId(HTTPStreamerCupertinoLiveStreamPacketizerChunkIdContext chunkIdContext)
	 {
	  return offset + chunkIdContext.getChunkIndex();
	 }
}