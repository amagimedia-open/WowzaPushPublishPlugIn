package com.amagi.thunderstorm.wowza;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.wowza.util.IPacketFragment;
import com.wowza.util.PacketFragmentList;
import com.wowza.wms.amf.AMFData;
import com.wowza.wms.amf.AMFDataList;
import com.wowza.wms.amf.AMFDataObj;
import com.wowza.wms.amf.AMFPacket;
import com.wowza.wms.httpstreamer.cupertinostreaming.httpstreamer.CupertinoStreamingRendition;
import com.wowza.wms.httpstreamer.cupertinostreaming.livestreampacketizer.CupertinoChunkMap;
import com.wowza.wms.httpstreamer.cupertinostreaming.livestreampacketizer.CupertinoChunkMapItem;
import com.wowza.wms.httpstreamer.cupertinostreaming.livestreampacketizer.CupertinoTSHolder;
import com.wowza.wms.httpstreamer.cupertinostreaming.livestreampacketizer.LiveStreamPacketizerCupertinoChunk;
import com.wowza.wms.logging.WMSLoggerFactory;
import com.wowza.wms.manifest.model.m3u8.MediaSegmentModel;
import com.wowza.wms.manifest.model.m3u8.PlaylistModel;
import com.wowza.wms.manifest.model.m3u8.tag.TagModel;
import com.wowza.wms.manifest.model.m3u8.tag.TagModelINF;
import com.wowza.wms.manifest.model.m3u8.tag.TagModelMediaSequence;
import com.wowza.wms.manifest.model.m3u8.tag.TagModelTargetDuration;
import com.wowza.wms.manifest.writer.m3u8.PlaylistWriter;
import com.wowza.wms.pushpublish.protocol.cupertino.PushPublishHTTPCupertino;
import com.wowza.wms.server.LicensingException;
import com.wowza.wms.util.PushPublishUtils;

public class PushPublishHTTPCupertinoChunkSplitterFileHandler extends PushPublishHTTPCupertino {

	private static final Class<PushPublishHTTPCupertinoChunkSplitterFileHandler> CLASS = PushPublishHTTPCupertinoChunkSplitterFileHandler.class;
	private static final String CLASSNAME = "PushPublishHTTPCupertinoChunkSplitterFileHandler";

	public static final String EVENTNAME_ONCUE = "onCUE";

	public static final int SPLICETYPE_OUT = 1;
	public static final int SPLICETYPE_IN = 2;

	public static final int SPLICEAPPENDDATA_OPERATION_PREPEND = 1;
	public static final int SPLICEAPPENDDATA_OPERATION_APPEND = 2;

	public static final String SPLICECOMMAND_INSERT = "insert";

	class SpliceEvent {
		protected long eventId = 0;
		protected long breakDuration = 0;
		protected int spliceType = 0;
		protected long currTime = 0;
		protected String base64Str = null;

		SpliceEvent(long eventId, long breakDuration, int spliceType, String uBase64Str) {
			this.eventId = eventId;
			this.breakDuration = breakDuration;
			this.spliceType = spliceType;
			this.currTime = System.currentTimeMillis();
			this.base64Str = uBase64Str;
		}
	}

	class SpliceLocation {
		protected long timecode = 0;
		protected SpliceEvent spliceEvent = null;

		public SpliceLocation(SpliceEvent spliceEvent, long timecode) {
			this.spliceEvent = spliceEvent;
			this.timecode = timecode;
		}
	}

	class SpliceChunkInfo {
		protected long chunkIndex = 0;
		protected long durationAdjustment = 0;
		protected SpliceEvent spliceEvent = null;

		public SpliceChunkInfo(long chunkIndex) {
			this.chunkIndex = chunkIndex;
		}
	}

	class SpliceAppendData {
		protected int operation = 0;
		protected long chunkIndex = 0;
		protected MediaSegmentModel mediaSegmentSource = null;
		protected CupertinoChunkMap chunkMap = null;
		protected CupertinoChunkMapItem mapItem = null;
		protected SpliceEvent spliceEvent = null;
		protected PacketFragmentList fragmentList = null;
		protected long durationMove = 0;
	}

	class PlaylistHolder {
		protected PlaylistModel playlist = null;
		protected String destinationFilePath = null;
		protected String groupName = null;

		PlaylistHolder(PlaylistModel playlist, String destinationFilePath, String groupName) {
			this.playlist = playlist;
			this.destinationFilePath = destinationFilePath;
			this.groupName = groupName;
		}

		PlaylistHolder(PlaylistModel playlist, String destinationFilePath) {
			this.playlist = playlist;
			this.destinationFilePath = destinationFilePath;
		}
	}

	class MediaSegmentHolder {
		protected MediaSegmentModel mediaSegment = null;
		protected String destinationFilePath = null;

		public MediaSegmentHolder(MediaSegmentModel mediaSegment, String destinationFilePath) {
			this.mediaSegment = mediaSegment;
			this.destinationFilePath = destinationFilePath;
		}
	}

	class MediaBreakInfo {
		String ScteEventStr;
		float breakDur;
		long chunkIdBreakBegin, chunkIdBreakEnd;
		String base64String;
		Hashtable<Long, TagModel> mTableTags;
		Hashtable<Long, Float> mTableDurations;

		MediaBreakInfo(float duration, long breakBeginMediaSeq, String uBase64String) {
			breakDur = duration;
			ScteEventStr = null;
			chunkIdBreakBegin = breakBeginMediaSeq;
			this.base64String = uBase64String;
			chunkIdBreakEnd = Long.MAX_VALUE;
			mTableTags = new Hashtable<Long, TagModel>();
			mTableDurations = new Hashtable<Long, Float>();
		}

		float getAggregatedDuration(long currentmediasequence) {
			float aggregate = 0;
			Iterator<Map.Entry<Long, Float>> it = mTableDurations.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry<Long, Float> entry = it.next();
				if (entry.getKey() < currentmediasequence) {
					aggregate = aggregate + entry.getValue();
				}
			}
			return aggregate;
		}

		void setendBreakChunk(long uLastChunk) {
			chunkIdBreakEnd = uLastChunk;
		}

		void setCONTTag(long mediaSeq, TagModel contTAG) {
			mTableTags.put(mediaSeq, contTAG);
		}

	}

	HashMap<String, MediaBreakInfo> mapBreakInfo = new HashMap<>();
	protected File rootDir = null;
	protected boolean backup = false;
	protected String groupName = null;

	protected PlaylistHolder pendingGroupMasterPlaylist = null;
	protected PlaylistHolder pendingMasterPlaylist = null;
	protected PlaylistHolder pendingMediaPlaylist = null;
	protected MediaSegmentHolder pendingMediaSegment = null;
	protected MediaSegmentHolder pendingDeleteMediaSegment = null;

	protected int spliceEventIdsHandledMax = 100;
	protected List<String> spliceEventIdsHandled = new ArrayList<String>();
	protected Map<Long, SpliceLocation> spliceLocations = new HashMap<Long, SpliceLocation>();
	protected Map<Long, SpliceChunkInfo> spliceChunkInfoMap = new HashMap<Long, SpliceChunkInfo>();
	protected List<SpliceAppendData> spliceAppendDataList = new ArrayList<SpliceAppendData>();

	public PushPublishHTTPCupertinoChunkSplitterFileHandler() throws LicensingException {
		super();
		this.keepMediaSegmentDataAfterSending = true;
	}

	@Override
	public void load(HashMap<String, String> dataMap) {
		super.load(dataMap);

		String destStr = PushPublishUtils.removeMapString(dataMap, "file.root");
		if (destStr != null) {
			this.rootDir = new File(destStr);
			logInfo("load", "Using: " + this.rootDir);
			if (!this.rootDir.exists()) {
				this.rootDir.mkdir();
				logInfo("load", "Created destination folder: " + this.rootDir);
			}
		}
	}

	@Override
	public boolean updateGroupMasterPlaylistPlaybackURI(String groupName, PlaylistModel masterPlaylist) {
		boolean retVal = true;
		String newPath = /* "../" + */ groupName + "/" + masterPlaylist.getUri().getPath();
		try {
			masterPlaylist.setUri(new URI(newPath));
			this.groupName = groupName;
		} catch (Exception e) {
			WMSLoggerFactory.getLogger(CLASS)
					.error(CLASSNAME + ".updateGroupMasterPlaylistPlaybackURI: Invalid path " + newPath, e);
			retVal = false;
		}
		return retVal;
	}

	@Override
	public boolean updateMasterPlaylistPlaybackURI(PlaylistModel playlist) {
		boolean retVal = true;

		String path = /* "../" + */ getDstStreamName() + (this.backup ? "-b/" : "/") + playlist.getUri().toString();
		try {
			playlist.setUri(new URI(path));
		} catch (URISyntaxException e) {
			WMSLoggerFactory.getLogger(CLASS).error(
					CLASSNAME + ".updateMasterPlaylistPlaybackURI: Failed to update master playlist to " + path, e);
			retVal = false;
		}
		return retVal;
	}

	@Override
	public boolean updateMediaPlaylistPlaybackURI(PlaylistModel playlist) {
		boolean retVal = true;

		String path = /* "../" + */ getDstStreamName() + (this.backup ? "-b/" : "/") + playlist.getUri().toString();
		try {
			playlist.setUri(new URI(path));
		} catch (URISyntaxException e) {
			WMSLoggerFactory.getLogger(CLASS).error(
					CLASSNAME + ".updateMediaPlaylistPlaybackURI: Failed to update media playlist to " + path, e);
			retVal = false;
		}
		return retVal;
	}

	@Override
	public boolean updateMediaSegmentPlaybackURI(MediaSegmentModel mediaSegment) {
		boolean retVal = true;
		String newPath = mediaSegment.getUri().getPath();

		// to prevent overriding prior segements if the stream were to reset,
		// we'll use the sessionStr to create a sub directory to keep the
		// media segments in.

		try {
			String temp = getRandomSessionStr() + "/" + newPath;
			mediaSegment.setUri(new URI(temp));
		} catch (Exception e) {
			retVal = false;
			WMSLoggerFactory.getLogger(CLASS)
					.error(CLASSNAME + ".updateMediaSegmentPlaybackURI: Invalid path " + newPath, e);
		}
		return retVal;
	}

	private int writePlaylist(PlaylistModel playlist, FileOutputStream output) throws IOException {
		int retVal = 0;
		ByteArrayOutputStream out = new ByteArrayOutputStream();

		PlaylistWriter writer = new PlaylistWriter(out, getContextStr());
		if (writer.write(playlist)) {
			String outStr = out.toString();
			byte[] bytes = outStr.getBytes();
			output.write(bytes);
			retVal = bytes.length;
		}

		return retVal;
	}

	private int writePlaylist(PlaylistHolder playlistHolder) {
		int retVal = 0;
		FileOutputStream output = null;
		try {
			File playlistFile = new File(playlistHolder.destinationFilePath);
			if (!playlistFile.exists())
				playlistFile.createNewFile();

			output = new FileOutputStream(playlistFile, false); // don't append
			retVal = writePlaylist(playlistHolder.playlist, output);
		} catch (Exception e) {
			WMSLoggerFactory.getLogger(CLASS).error(CLASSNAME + ".writePlaylist: Failed to send master playlist to: "
					+ playlistHolder.destinationFilePath, e);
		} finally {
			if (output != null)
				try {
					output.flush();
					output.close();
				} catch (Exception e2) {

				}
			;
		}
		return retVal;
	}

	@Override
	public int sendGroupMasterPlaylist(String groupName, PlaylistModel playlist) {
		File destinationDir = getDestionationGroupDir();
		if (!destinationDir.exists())
			destinationDir.mkdirs();

		String destinationFilePath = destinationDir + "/" + playlist.getUri();

		// we are holding back on chunk for splicing - do the same for playlists
		pendingGroupMasterPlaylist = new PlaylistHolder(playlist, destinationFilePath, groupName);

		return 1;
	}

	@Override
	public int sendMasterPlaylist(PlaylistModel playlist) {
		File destinationDir = getDestionationDir();
		if (!destinationDir.exists())
			destinationDir.mkdirs();

		String destinationFilePath = destinationDir + "/" + playlist.getUri();

		// we are holding back on chunk for splicing - do the same for playlists
		pendingMasterPlaylist = new PlaylistHolder(playlist, destinationFilePath);

		return 1;
	}

	// Adjust the durations and target duration of media playlist
	private void adjustMediaPlaylist(PlaylistModel playlist) {
		try {
			long chunkIdCurr = 0;
			TagModelTargetDuration tagTargetDuration = null;

			double maxDuration = 0.0;
			List<TagModel> tags = playlist.tags;

			class TagHolder {
				TagModel tag = null;
				int index = 0;

				public TagHolder(TagModel tag, int index) {
					this.tag = tag;
					this.index = index;
				}
			}

			LinkedList<TagHolder> tagsToAdd = new LinkedList<TagHolder>();

			int tagIndex = 0;
			for (TagModel tag : tags) {
				if (tag instanceof TagModelMediaSequence) {
					TagModelMediaSequence tagMediaSequence = (TagModelMediaSequence) tag;

					// since 4.7.5.02, chunk ids are long values but TagModelMediaSequence hasn't
					// been updated so there may be some weird results if the chunk id exceeds
					// Integer.MAX_VALUE.
					chunkIdCurr = tagMediaSequence.getNumber().longValue();
				} else if (tag instanceof TagModelTargetDuration) {
					tagTargetDuration = (TagModelTargetDuration) tag;
				} else if (tag instanceof TagModelINF) {
					TagModelINF tagINF = (TagModelINF) tag;

					Object durationObj = tagINF.getDuration();

					float duration = 0;
					if (durationObj instanceof Integer)
						duration = ((Integer) durationObj).intValue();
					else if (durationObj instanceof Float)
						duration = ((Float) durationObj).floatValue();

					SpliceChunkInfo chunkInfo = spliceChunkInfoMap.get(new Long(chunkIdCurr));
					if (chunkInfo != null) {
						// update chunk durations if needed due to splicing
						if (chunkInfo.durationAdjustment != 0) {
							// trying to be precise to the millisecond

							long durationLong = Math.round(duration * 1000) + chunkInfo.durationAdjustment;

							String decimalStr = (durationLong % 1000) + "";
							while (decimalStr.length() < 3)
								decimalStr = "0" + decimalStr;

							duration = Float.parseFloat((durationLong / 1000) + "." + decimalStr);
						}

						// add CUE-OUT and CUE-IN headers
						if (chunkInfo.spliceEvent != null) {

							WMSLoggerFactory.getLogger(CLASS).info(CLASSNAME + ".adjustMediaPlaylist: "
									+ "Found Splice Event" + chunkInfo.spliceEvent.eventId);

							switch (chunkInfo.spliceEvent.spliceType) {
							case SPLICETYPE_OUT: {
								/*
								 * Add CUE OUT Heading marking begining of break. This is expected in all cases
								 * supported. This may or may not be accompanied with CUE IN, indicating end of
								 * break.
								 */
								MediaBreakInfo uBreakInfo = mapBreakInfo.get(playlist.getUri().toString());
								if (uBreakInfo != null) {
									mapBreakInfo.remove(playlist.getUri().toString());
									uBreakInfo = null;
								}
								uBreakInfo = new MediaBreakInfo((float) (chunkInfo.spliceEvent.breakDuration / 1000),
										chunkIdCurr, chunkInfo.spliceEvent.base64Str);

								mapBreakInfo.put(playlist.getUri().toString(), uBreakInfo);

								// Add OATCLS and increment index
								TagModel tagNew = new TagModelOATCLSSCTE35(chunkInfo.spliceEvent.base64Str);
								TagHolder tagHolder = new TagHolder(tagNew, tagIndex);
								tagsToAdd.add(tagHolder);
								// Add CUE Out
								tagNew = new TagModelCueOut(chunkInfo.spliceEvent.breakDuration);
								tagHolder = new TagHolder(tagNew, tagIndex);
								tagsToAdd.add(tagHolder);
							}
								break;
							case SPLICETYPE_IN: {

								/*
								 * This signals end of break, This is expected in all cases supported. DO NOT
								 * add CUE IN TAG in playlist now,we are adding it below now That is done, so
								 * that we handle cases, when CUE in is received from ingest Also in case when
								 * it is NOT sent explicitly.
								 * 
								 */

								// TagModel tagNew = new TagModelCueIn();
								// TagHolder tagHolder = new TagHolder(tagNew, tagIndex);
								// tagsToAdd.add(tagHolder);

								// Mark end of break and beginning of content instead.
								MediaBreakInfo uBreakInfo = mapBreakInfo.get(playlist.getUri().toString());
								if (uBreakInfo != null) {
									uBreakInfo.setendBreakChunk(chunkIdCurr);
								}
							}
								break;
							}

						}
					}

					if (duration > maxDuration)
						maxDuration = duration;

					tagINF.setDuration(new Float(duration));

					MediaBreakInfo uBreakInfo = mapBreakInfo.get(playlist.getUri().toString());
					if (uBreakInfo != null && (chunkIdCurr > uBreakInfo.chunkIdBreakBegin
							&& chunkIdCurr <= uBreakInfo.chunkIdBreakEnd)) {
						/*
						 * Means we are in break for THIS variant and between segments after BEGIN of
						 * break Before end of break.
						 */
						TagModel tagNew = uBreakInfo.mTableTags.get(chunkIdCurr);
						if (tagNew == null) {
							float elapsedDur = uBreakInfo.getAggregatedDuration(chunkIdCurr);
							WMSLoggerFactory.getLogger(CLASS)
									.info(String.format("Amagi: elapsedDur:%f,duration:%f,media seq:%d,url=%s",
											elapsedDur, duration, chunkIdCurr, playlist.getUri().toString()));

							if ((uBreakInfo.breakDur != 0 && (elapsedDur + 0.5) >= uBreakInfo.breakDur)
									|| (chunkIdCurr == uBreakInfo.chunkIdBreakEnd)) {
								// Do this when break duration is NON ZERO and if elapsed time
								// has reached break duration,it means playout had no plans of putting
								// CUE In

								// Add CUE IN We are done.
								tagNew = new TagModelCueIn();
								uBreakInfo.setCONTTag(chunkIdCurr, tagNew);
								uBreakInfo.setendBreakChunk(chunkIdCurr);
							} else {

								// If break duration is ZERO, put CUE-OUT-CONT until we get CUE IN
								// If break duration is NON ZERO, put CUE-OUT-CONT until
								// we elapsed time < break dur

								// ADD CUE-OUT-CONT
								tagNew = new TagModelCueOutCont(elapsedDur, uBreakInfo.breakDur,
										uBreakInfo.base64String);
								uBreakInfo.setCONTTag(chunkIdCurr, tagNew);

							}
						}
						TagHolder tagHolder = new TagHolder(tagNew, tagIndex);
						tagsToAdd.add(tagHolder);
					}

					// Maintain MAP of CHUNK ID and DURATION for period of break.
					if (uBreakInfo != null
							&& (chunkIdCurr >= uBreakInfo.chunkIdBreakBegin && chunkIdCurr < uBreakInfo.chunkIdBreakEnd)
							&& uBreakInfo.mTableDurations.get(chunkIdCurr) == null) {

						uBreakInfo.mTableDurations.put(chunkIdCurr, duration);
					}

					chunkIdCurr++;
				}

				tagIndex++;
			}
			// Amagi: We didn't see tags list updated with CUE information here
			Iterator<TagHolder> iter = tagsToAdd.descendingIterator();
			while (iter.hasNext()) {
				TagHolder tagHolder = iter.next();
				tags.add(tagHolder.index, tagHolder.tag);
			}

			// update target duration
			if (tagTargetDuration != null) {
				int targetDuration = tagTargetDuration.getTargetDuration().intValue();
				int targetDurationNew = (int) Math.ceil(maxDuration);

				if (targetDurationNew > targetDuration)
					tagTargetDuration.setTargetDuration(new Integer(targetDurationNew));
			}

		} catch (Exception e) {
			WMSLoggerFactory.getLogger(CLASS).error(CLASSNAME + ".adjustMediaPlaylist: ", e);
		}
	}

	@Override
	public int sendMediaPlaylist(PlaylistModel playlist) {
		File destinationDir = getDestionationDir();
		if (!destinationDir.exists())
			destinationDir.mkdirs();

		String destinationFilePath = destinationDir + "/" + playlist.getUri();

		PlaylistHolder sendMediaPlaylist = pendingMediaPlaylist;

		// we are holding back on chunk for splicing - do the same for playlists
		pendingMediaPlaylist = new PlaylistHolder(playlist, destinationFilePath, this.groupName);

		if (sendMediaPlaylist != null) {
			adjustMediaPlaylist(sendMediaPlaylist.playlist);

			writePlaylist(sendMediaPlaylist);

			if (pendingMasterPlaylist != null) {
				writePlaylist(pendingMasterPlaylist);
				pendingMasterPlaylist = null;
			}

			if (pendingGroupMasterPlaylist != null) {
				writePlaylist(pendingGroupMasterPlaylist);
				pendingGroupMasterPlaylist = null;
			}
		}

		return 1;
	}

	private int writeFragments(PacketFragmentList list, FileOutputStream output) throws IOException {
		int bytesWritten = 0;

		Iterator<IPacketFragment> itr = list.getFragments().iterator();
		while (itr.hasNext()) {
			IPacketFragment fragment = itr.next();
			if (fragment.getLen() <= 0)
				continue;
			byte[] data = fragment.getBuffer();

			output.write(data);
			bytesWritten += data.length;
		}

		return bytesWritten;
	}

	private int writeMediaSegment(MediaSegmentHolder mediaSegmentHolder) {
		int bytesWritten = 0;
		FileOutputStream output = null;
		try {
			File file = new File(mediaSegmentHolder.destinationFilePath);
			if (!file.exists())
				file.createNewFile();

			PacketFragmentList list = mediaSegmentHolder.mediaSegment.getFragmentList();
			if (list != null) {
				output = new FileOutputStream(file, false);
				bytesWritten = writeFragments(list, output);
			}
		} catch (Exception e) {
			WMSLoggerFactory.getLogger(CLASS)
					.error(CLASSNAME + ".writeMediaSegment: Failed to send media segment data to "
							+ mediaSegmentHolder.destinationFilePath, e);
		} finally {
			if (output != null) {
				try {
					output.flush();
					output.close();
				} catch (Exception e) {
				}
			}
		}

		return bytesWritten;
	}

	// inspect data events for each chunk and look for onCUE events - interpret the
	// splice points
	private void extractSplicePoints(MediaSegmentModel mediaSegment) {
		try {
			LiveStreamPacketizerCupertinoChunk chunkInfo = (LiveStreamPacketizerCupertinoChunk) mediaSegment
					.getChunkInfoCupertino();
			if (chunkInfo != null) {
				chunkInfo.getRendition();
				List<AMFPacket> dataPackets = chunkInfo.getDataPackets();

				if (dataPackets != null && dataPackets.size() > 0) {
					for (AMFPacket packet : dataPackets) {
						try {
							while (true) {
								byte[] buffer = packet.getData();
								if (buffer == null) {
									WMSLoggerFactory.getLogger(CLASS)
											.warn(CLASSNAME + ".sendMediaSegment: AMF data packet, buffer null");
									break;
								}

								AMFDataList amfDataList = new AMFDataList(buffer);

								if (amfDataList.size() < 1 || amfDataList.getType(0) != AMFData.DATA_TYPE_STRING)
									break;

								// looking for onCUE events
								String eventStr = amfDataList.getString(0);
								AMFDataObj cueObj = amfDataList.getObject(1);
								if (!EVENTNAME_ONCUE.equals(eventStr))
									break;

								if (amfDataList.size() < 2 || amfDataList.getType(1) != AMFData.DATA_TYPE_OBJECT) {
									WMSLoggerFactory.getLogger(CLASS).warn(CLASSNAME
											+ ".sendMediaSegment: AMF data packet, incorrect AMFList data [second item not object]");
									break;
								}

								/*
								 * AMFDataList: [0] onCUE, [1] object {Obj[]: version: 1.0, protocolVersion:
								 * 0.0, encryptedPacket: false, encryptionAlgorithm: 0.0, ptsAdjustment: 0.0,
								 * cwIndex: 255.0, encryptedCRC: -1.0, command: {Obj[]: SpliceCommand: "insert",
								 * event: {Obj[]: eventID: 1.073745171E9, cancel: false, outOfNetwork: true,
								 * programSplice: true, durationFlag: true, spliceImmediate: false, programID:
								 * 0.0, availNum: 0.0, availsExpected: 0.0, breakDuration: 9270000.0,
								 * breakDurationAutoReturn: false, spliceTime: {Obj[]: isSpecified: true, isUTC:
								 * false, spliceTime: 2.244440255E9, spliceTimeMS: 9.7854748856E10},
								 * componentSplices: {Obj[]: }}}, descriptor: {Obj[]: 0:
								 * "Descriptor (0) : (8) "}}
								 */

								AMFDataObj commandObj = cueObj.getObject("command");
								if (commandObj == null) {
									WMSLoggerFactory.getLogger(CLASS)
											.warn(CLASSNAME + ".sendMediaSegment: onCUE, command obj missing");
									break;
								}

								String spliceCommand = commandObj.getString("SpliceCommand");
								if (!SPLICECOMMAND_INSERT.equals(spliceCommand)) {
									WMSLoggerFactory.getLogger(CLASS).warn(CLASSNAME
											+ ".sendMediaSegment: onCUE, incorrect splice comamnd: " + spliceCommand);
									break;
								}

								AMFDataObj eventObj = commandObj.getObject("event");
								if (eventObj == null) {
									WMSLoggerFactory.getLogger(CLASS)
											.warn(CLASSNAME + ".sendMediaSegment: onCUE, event obj missing");
									break;
								}

								AMFDataObj spliceTimeObj = eventObj.getObject("spliceTime");
								if (spliceTimeObj == null) {
									WMSLoggerFactory.getLogger(CLASS)
											.warn(CLASSNAME + ".sendMediaSegment: onCUE, spliceTime obj missing");
									break;
								}

								long eventId = eventObj.getLong("eventID");
								boolean outOfNetwork = eventObj.getBoolean("outOfNetwork");
								long ptsAdjustment = cueObj.getLong("ptsAdjustment");
								long breakDuration = eventObj.getLong("breakDuration");
								long spliceTime = spliceTimeObj.getLong("spliceTime");
								long spliceTimeMS = spliceTimeObj.getLong("spliceTimeMS");
								String uSCTEb64 = cueObj.getString("SCTEb64");

								String eventIdObj = eventId + ":" + outOfNetwork + ":" + ptsAdjustment + ":"
										+ breakDuration + ":" + spliceTime; // factor out duplicated

								WMSLoggerFactory.getLogger(CLASS)
										.info(CLASSNAME + "extractSplicePoints: Recovered SCTE Splice Insert Event:"
												+ eventIdObj + uSCTEb64);

								// if not an event we have already processed - TS stream could contain duplicate
								// events
								if (!spliceEventIdsHandled.contains(eventIdObj)) {
									if (this.pushPublishStreamDebug)
										WMSLoggerFactory.getLogger(CLASS).info(CLASSNAME + ".extractSplicePoints["
												+ eventId + "]: obj:" + cueObj.toString());

									spliceEventIdsHandled.add(eventIdObj);
									while (spliceEventIdsHandled.size() > spliceEventIdsHandledMax) {
										spliceEventIdsHandled.remove(0);
									}

									int spliceType = (outOfNetwork ? SPLICETYPE_OUT : SPLICETYPE_IN);

									// add a splice event that we will use to split the chunk
									SpliceEvent spliceEvent = new SpliceEvent(eventId, breakDuration / 90, spliceType,
											uSCTEb64);
									SpliceLocation spliceLocation = new SpliceLocation(spliceEvent,
											spliceTimeMS + (ptsAdjustment / 90));

									spliceLocations.put(new Long(spliceLocation.timecode), spliceLocation);

									WMSLoggerFactory.getLogger(CLASS)
											.info(CLASSNAME + ".extractSplicePoints: spliceEvent[id:" + eventId + ":"
													+ (outOfNetwork ? "out" : "in") + "]: tc:" + spliceLocation.timecode
													+ " duration:" + (spliceEvent.breakDuration / 90));
								} else {

									WMSLoggerFactory.getLogger(CLASS).info(CLASSNAME
											+ "extractSplicePoints: Found Duplicate SCTE event: ID" + eventId);
								}
								break;
							}
						} catch (Exception e) {
							WMSLoggerFactory.getLogger(CLASS).error(CLASSNAME + ".extractSplicePoints: ", e);
						}
					}
				}
			}
		} catch (Exception e) {
			WMSLoggerFactory.getLogger(CLASS).error(CLASSNAME + ".extractSplicePoints: ", e);
		}
	}

	private int getPMTOffset(CupertinoChunkMap chunkMap) {
		int offset = 0;

		List<CupertinoChunkMapItem> items = chunkMap.getItems();
		for (CupertinoChunkMapItem item : items) {
			int tsType = item.getTSType();

			if (tsType == CupertinoTSHolder.TSTYPE_PATPMT) {
				offset = item.getOffset() + item.getLen();
				break;
			}
		}

		return offset;
	}

	// append or prepend data to this segment if needed
	private void appendSegementData(MediaSegmentModel mediaSegment) {
		try {
			if (spliceAppendDataList.size() > 0) {
				Iterator<SpliceAppendData> iter = spliceAppendDataList.iterator();

				while (iter.hasNext()) {
					SpliceAppendData spliceAppendData = iter.next();

					LiveStreamPacketizerCupertinoChunk chunk = (LiveStreamPacketizerCupertinoChunk) mediaSegment
							.getChunkInfoCupertino();
					if (chunk != null) {
						long chunkIndex = chunk.getChunkIndexForPlaylist();

						SpliceChunkInfo spliceChunkInfo = spliceChunkInfoMap.get(new Long(chunkIndex));

						// this is the chunk to which we need to append/prepend data
						if (spliceChunkInfo != null && chunkIndex == spliceAppendData.chunkIndex) {
							iter.remove();

							// this chunk is beginning of the splice event
							if (spliceAppendData.spliceEvent != null)
								spliceChunkInfo.spliceEvent = spliceAppendData.spliceEvent;

							// adjust the chunk duration for the spliced data
							spliceChunkInfo.durationAdjustment += spliceAppendData.durationMove;

							// append/prepend the data to the chunk
							PacketFragmentList fragmentList = mediaSegment.getFragmentList().clone();

							List<IPacketFragment> fragments = spliceAppendData.fragmentList.getFragments();
							switch (spliceAppendData.operation) {
							case SPLICEAPPENDDATA_OPERATION_APPEND:
								for (IPacketFragment fragment : fragments) {
									fragmentList.addPacketFragment(fragment);
								}
								break;
							case SPLICEAPPENDDATA_OPERATION_PREPEND:
								int insertLoc = 0;
								for (IPacketFragment fragment : fragments) {
									fragmentList.addPacketFragment(insertLoc, fragment);
									insertLoc++;
								}
								break;
							}

							mediaSegment.setFragmentList(fragmentList);
						}
					}
				}
			}
		} catch (Exception e) {
			WMSLoggerFactory.getLogger(CLASS).error(CLASSNAME + ".appendSegementData: ", e);
		}
	}

	// see if splice points are in the current chunk
	private void handleSplicePoints(MediaSegmentModel mediaSegment, LiveStreamPacketizerCupertinoChunk chunk,
			SpliceChunkInfo spliceChunkInfo) {
		try {
			long chunkIndex = chunk.getChunkIndexForPlaylist();

			Iterator<Map.Entry<Long, SpliceLocation>> iter = spliceLocations.entrySet().iterator();

			while (iter.hasNext()) {
				Map.Entry<Long, SpliceLocation> entry = iter.next();

				long timecode = entry.getKey().longValue();
				SpliceLocation spliceLocation = entry.getValue();

				long durationTotal = chunk.getDuration();

				// is splice point in current chunk
				if (timecode >= chunk.getStartTimecode() && timecode < (chunk.getStartTimecode() + durationTotal)) {
					iter.remove();

					CupertinoChunkMap chunkMap = chunk.getChunkMap();
					List<CupertinoChunkMapItem> items = chunkMap.getItems();

					// find the splice point in the chunk map
					int tolerance = 10;
					int videoIndex = 0;
					CupertinoChunkMapItem itemLast = null;
					CupertinoChunkMapItem itemMatch = null;
					int videoIndexMatch = 0;
					for (CupertinoChunkMapItem item : items) {
						int tsType = item.getTSType();
						if (tsType == CupertinoTSHolder.TSTYPE_VIDEO) {
							int diff = (int) Math.abs(item.getTimecode() - timecode);
							if (diff <= tolerance) {
								itemMatch = item;
								videoIndexMatch = videoIndex;
							} else if (item.getTimecode() > timecode) {
								itemMatch = itemLast;
								videoIndexMatch = videoIndex - 1;
							}

							if (itemMatch != null) {
								WMSLoggerFactory.getLogger(CLASS).info(CLASSNAME + ".extractSplicePoints: foundIt["
										+ chunk.getChunkIndex() + "]: " + diff + ":" + tolerance);
								break;
							}

							videoIndex++;
							itemLast = item;
						}
					}

					// of no match then must be last item in the list
					if (itemMatch == null)
						itemMatch = itemLast;

					if (itemMatch != null) {
						long durationMove = (itemMatch.getTimecode() - chunk.getStartTimecode());

						// if not the first item in the chunk - break the chunk and the splice point
						if (videoIndexMatch > 0 && durationMove > 0) {
							SpliceAppendData spliceAppendData = new SpliceAppendData();

							PacketFragmentList fragmentList = mediaSegment.getFragmentList();
							PacketFragmentList fragmentListAppend = fragmentList.clone();
							int pmtOffset = getPMTOffset(chunkMap);
							int fragmentListTotalSize = fragmentList.size();

							// if splice point is in first 1/2 of the chunk append to previous chunk
							if (durationMove <= durationTotal / 2) {
								// append previous
								spliceAppendData.operation = SPLICEAPPENDDATA_OPERATION_APPEND;
								spliceAppendData.chunkIndex = chunkIndex - 1;

								PacketFragmentList fragmentListNew = fragmentList.clone();
								fragmentListNew.removeBytes(pmtOffset, itemMatch.getOffset() - pmtOffset);
								mediaSegment.setFragmentList(fragmentListNew);

								fragmentListAppend.removeBytes(itemMatch.getOffset(),
										fragmentListTotalSize - itemMatch.getOffset());
								fragmentListAppend.removeBytes(0, pmtOffset);

								spliceChunkInfo.durationAdjustment = -durationMove;
								spliceAppendData.durationMove = -spliceChunkInfo.durationAdjustment;

								spliceChunkInfo.spliceEvent = spliceLocation.spliceEvent; // this chunk has the splice
																							// event

								WMSLoggerFactory.getLogger(CLASS)
										.info(CLASSNAME + "handleSplicePoints(Prev): event-ID"
												+ spliceLocation.spliceEvent.eventId + "Media Sequence:"
												+ spliceAppendData.chunkIndex + "spliceAppendData updated");
							} else // if splice point is in second 1/2 of the chunk prepend to next chunk
							{
								// prepend next
								spliceAppendData.operation = SPLICEAPPENDDATA_OPERATION_PREPEND;
								spliceAppendData.chunkIndex = chunkIndex + 1;

								PacketFragmentList fragmentListNew = fragmentList.clone();
								fragmentListNew.removeBytes(itemMatch.getOffset(),
										fragmentListTotalSize - itemMatch.getOffset());
								mediaSegment.setFragmentList(fragmentListNew);

								fragmentListAppend.removeBytes(pmtOffset, itemMatch.getOffset() - pmtOffset);

								spliceChunkInfo.durationAdjustment = -(durationTotal - durationMove);
								spliceAppendData.durationMove = -spliceChunkInfo.durationAdjustment;

								spliceAppendData.spliceEvent = spliceLocation.spliceEvent; // next chunk has splice
																							// event

								WMSLoggerFactory.getLogger(CLASS)
										.info(CLASSNAME + "handleSplicePoints(Next): event-ID"
												+ spliceLocation.spliceEvent.eventId + "Media Sequence:"
												+ spliceAppendData.chunkIndex + "spliceAppendData updated");
							}

							spliceAppendData.mediaSegmentSource = mediaSegment;
							spliceAppendData.chunkMap = chunkMap;
							spliceAppendData.mapItem = itemMatch;
							spliceAppendData.fragmentList = fragmentListAppend;

							spliceAppendDataList.add(spliceAppendData);
						} else {
							WMSLoggerFactory.getLogger(CLASS)
									.info(CLASSNAME + "handleSplicePoints: event-ID"
											+ spliceLocation.spliceEvent.eventId
											+ "Not in 1st or 2nd half, found outside ?" + "spliceChunkInfo updated");

							spliceChunkInfo.spliceEvent = spliceLocation.spliceEvent;
						}

						WMSLoggerFactory.getLogger(CLASS).info(CLASSNAME + ".extractSplicePoints: durationMove["
								+ chunk.getChunkIndex() + "]:" + durationMove);
					}
				} else {
					WMSLoggerFactory.getLogger(CLASS).info(CLASSNAME + "handleSplicePoints: event-ID"
							+ spliceLocation.spliceEvent.eventId + "Not found in current chunk");
				}
			}
		} catch (Exception e) {
			WMSLoggerFactory.getLogger(CLASS).error(CLASSNAME + ".handleSplicePoints: ", e);
		}
	}

	// look for splice points in the stream and process accordingly
	private void processMediaSegmentEvents(MediaSegmentModel mediaSegment, MediaSegmentModel lastMediaSegment) {
		try {
			LiveStreamPacketizerCupertinoChunk chunk = (LiveStreamPacketizerCupertinoChunk) mediaSegment
					.getChunkInfoCupertino();
			if (chunk != null) {
				long chunkIndex = chunk.getChunkIndexForPlaylist();

				SpliceChunkInfo spliceChunkInfo = new SpliceChunkInfo(chunkIndex);

				spliceChunkInfoMap.put(new Long(chunkIndex), spliceChunkInfo);

				CupertinoStreamingRendition rendition = chunk.getRendition();

				if (rendition.isAudioVideo() || rendition.isVideoOnly()) {
					// extract splice points from the onCUE events
					extractSplicePoints(mediaSegment);

					// if there are splice points - see if they are in the current chunk
					if (spliceLocations.size() > 0)
						handleSplicePoints(mediaSegment, chunk, spliceChunkInfo);
				}
			}
		} catch (Exception e) {
			WMSLoggerFactory.getLogger(CLASS).error(CLASSNAME + ".processMediaSegmentEvents: ", e);
		}
	}

	@Override
	public int sendMediaSegment(MediaSegmentModel mediaSegment) {
		String destinationDir = getDestionationDir().toString() + "/" + getDstStreamName();
		String path = destinationDir + "/" + mediaSegment.getUri();
		// WMSLoggerFactory.getLogger(CLASS).info("Segment PATH " + path);
		int idx = path.lastIndexOf("/media_");
		path = path.substring(0, idx);
		File file = new File(path);

		if (!file.exists())
			file.mkdirs();

		String destinationFilePath = destinationDir + "/" + mediaSegment.getUri();

		MediaSegmentHolder sendMediaSegment = this.pendingMediaSegment;

		// hold back on chunk for splicing
		this.pendingMediaSegment = new MediaSegmentHolder(mediaSegment, destinationFilePath);

		int retVal = 0;
		if (sendMediaSegment != null) {
			// process data events looking for onCUE events and find splice points
			processMediaSegmentEvents(mediaSegment, sendMediaSegment.mediaSegment);

			// append/prepend data to current chunk
			appendSegementData(sendMediaSegment.mediaSegment);

			retVal += writeMediaSegment(sendMediaSegment);

			if (retVal > 0) {
				sendMediaSegment.mediaSegment.setFragmentList(null);
				sendMediaSegment.mediaSegment.setChunkInfoCupertino(null);
			}

			return retVal;
		}

		return 1;
	}

	@Override
	public int deleteMediaSegment(MediaSegmentModel mediaSegment) {
		int retVal = 0;

		try {
			// delete per-chunk data from spliceChunkInfoMap as chunk is deleted from the
			// list
			LiveStreamPacketizerCupertinoChunk chunk = (LiveStreamPacketizerCupertinoChunk) mediaSegment
					.getChunkInfoCupertino();
			if (chunk != null) {
				long chunkIndex = chunk.getChunkIndexForPlaylist();
				spliceChunkInfoMap.remove(new Long(chunkIndex));
			}
		} catch (Exception e) {
			WMSLoggerFactory.getLogger(CLASS).error(CLASSNAME + ".deleteMediaSegment: ", e);
		}

		MediaSegmentHolder deleteMediaSegment = this.pendingDeleteMediaSegment;

		// hold back deleting chunk
		this.pendingDeleteMediaSegment = new MediaSegmentHolder(mediaSegment, null);

		if (deleteMediaSegment != null) {
			File segment = new File(
					getDestionationDir() + "/" + getDstStreamName() + "/" + deleteMediaSegment.mediaSegment.getUri());
			if (segment.exists())
				if (segment.delete())
					retVal = 1;
		}
		return retVal;
	}

	@Override
	public void setSendToBackupServer(boolean backup) {
		this.backup = backup;
	}

	@Override
	public boolean isSendToBackupServer() {
		return this.backup;
	}

	@Override
	public boolean outputOpen() {
		return true;
	}

	@Override
	public boolean outputClose() {
		return true;
	}

	@Override
	public String getDestionationLogData() {
		File destinationDir = getDestionationDir();
		String retVal = "Invalid Destination " + destinationDir.toString();
		try {
			retVal = destinationDir.toURI().toURL().toString();
		} catch (MalformedURLException e) {
			WMSLoggerFactory.getLogger(CLASS).error(
					CLASSNAME + ".getDestionationLogData: Unable to convert " + destinationDir + " to valid path", e);
		}

		return retVal;
	}

	private File getDestionationDir() {
		if (!this.backup)
			// return new File(this.rootDir + "/" + getDstStreamName());
			return new File(this.rootDir + "/" + getAdaptiveGroupName());
		return new File(this.rootDir + "/" + "/" + getDstStreamName() + "-b");
	}

	private File getDestionationGroupDir() {
		if (!this.backup)
			// return new File(this.rootDir + "/" + this.groupName);
			return new File(this.rootDir.toString());
		return new File(this.rootDir + "/" + getDstStreamName() + "-b");
	}

}