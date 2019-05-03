package com.amagi.thunderstorm.wowza;

import java.util.ArrayList;
import java.util.zip.CRC32;

import javax.xml.bind.DatatypeConverter;

import com.wowza.wms.amf.AMFDataObj;
import com.wowza.wms.logging.WMSLoggerFactory;
import com.wowza.wms.rtp.depacketizer.RTPDePacketizerItem;
import com.wowza.wms.rtp.depacketizer.RTPDePacketizerMPEGTS;
import com.wowza.wms.rtp.depacketizer.RTPDePacketizerMPEGTSNotifyBase;
import com.wowza.wms.rtp.model.RTPContext;
import com.wowza.wms.rtp.model.RTPStream;
import com.wowza.wms.rtp.model.RTPTrack;
import com.wowza.wms.stream.IMediaStream;
import com.wowza.wms.transport.mpeg2.AdaptationField;
import com.wowza.wms.transport.mpeg2.Descriptor;
import com.wowza.wms.transport.mpeg2.IMPEG2UserMonitorSectionNotify;
import com.wowza.wms.transport.mpeg2.MPEG2Section;
import com.wowza.wms.transport.mpeg2.ProgramMapTable;
import com.wowza.wms.transport.mpeg2.ProgramMapTable.StreamInfo;
import com.wowza.wms.transport.mpeg2.RegistrationDescriptor;
import com.wowza.wms.transport.mpeg2.section.cue.SpliceInformationTable;
import com.wowza.wms.transport.mpeg2.section.cue.SpliceInformationTableSerializeAMFContext;
import com.wowza.wms.transport.mpeg2.section.cue.SpliceInsertCommand;

public class RTPDePacketizerMPEGTSMonitorCUE extends RTPDePacketizerMPEGTSNotifyBase {
	private static final Class<RTPDePacketizerMPEGTSMonitorCUE> CLASS = RTPDePacketizerMPEGTSMonitorCUE.class;
	private static final String CLASSNAME = "RTPDePacketizerMPEGTSMonitorCUE";

	private IMediaStream stream = null;
	private RTPDePacketizerMPEGTS rtDePacketizerMPEGTS = null;
	private boolean isTimecodeReady = false;
	private RTPTrack rtpTrack = null;
	private boolean debugLog = false;

	class MPEGTSMonitorCUE implements IMPEG2UserMonitorSectionNotify {
		private  final char[] CA = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/".toCharArray();
		@Override
		public void onMonitorStart() {
			WMSLoggerFactory.getLogger(CLASS).
			info("Successfully started monitoring for SCTE Events");
		}

		@Override
		public void onMonitorStop() {
			WMSLoggerFactory.getLogger(CLASS).info("Stopping the monitor for SCTE Events");			
		}

		@Override
		public void onDataSection(int pid, AdaptationField field, MPEG2Section section) {

			WMSLoggerFactory.getLogger(CLASS).info(".onMonitorDataSection: " + section.getTableID()
			+ "  SIT_TABLE_ID: " + SpliceInformationTable.SIT_TABLE_ID);
	
			if (section.getTableID() == SpliceInformationTable.SIT_TABLE_ID) {
				try {
				/*	SpliceInsertCommand spliceInsertCommand = new SpliceInsertCommand(section);
					if (spliceInsertCommand != null) {
						WMSLoggerFactory.getLogger(CLASS).info("Detected a SCTE splice point, need to change code");
					}*/

					
					SpliceInformationTable spliceInformationTable = new SpliceInformationTable(section);
					if (spliceInformationTable != null) {
						
						String SCTE = getSCTEString(section.dumpSection(""));
						if(SCTE!=null) {
							WMSLoggerFactory.getLogger(CLASS).info("Detected a SCTE string"+SCTE);
						}
						
						if (debugLog)
							WMSLoggerFactory.getLogger(CLASS)
									.info(CLASSNAME + ".onDataSection: " + spliceInformationTable.toString());

						SpliceInformationTableSerializeAMFContext serializeContext = new SpliceInformationTableSerializeAMFContext();

						serializeContext.timeReference = rtDePacketizerMPEGTS.getVideoTC();
						serializeContext.rtpTrack = rtpTrack;

						AMFDataObj amfData = spliceInformationTable.serializeAMF(serializeContext);
						amfData.put("SCTEb64",SCTE);
						if (amfData != null) {
							WMSLoggerFactory.getLogger(CLASS)
									.info(CLASSNAME + ".onDataSection: " + "Inserting onCue as AMF Events");
							stream.sendDirect("onCUE", amfData);
						}
					}

				} catch (Exception e) {
					WMSLoggerFactory.getLogger(CLASS).error(CLASSNAME + ".onDataSection: ", e);
				}
			}
		}
		private String getSCTEString(String sectionDump) {
			sectionDump = sectionDump.replaceAll("mpeg2Section:dumpSection", "").replaceAll("\n", "").replaceAll(" ", "")
					.replaceAll(":", "");
			WMSLoggerFactory.getLogger(CLASS).info("Hex String " + sectionDump);
			CRC32 crc32 = new CRC32();
			crc32.update(sectionDump.getBytes());
			sectionDump = sectionDump + Long.toHexString(crc32.getValue()).toUpperCase();
			// pad the string if it is not of even length
			if (sectionDump.length() % 2 != 0) {
				sectionDump = sectionDump + "0";
			}
			byte[] bArr = DatatypeConverter.parseHexBinary(sectionDump);
			String Base64EncodeString = encodeToString(bArr, false);
			WMSLoggerFactory.getLogger(CLASS).info("Base64 SCTE String :" + Base64EncodeString);
			return Base64EncodeString;
		}
		private final char[] encodeToChar(byte[] sArr, boolean lineSep) {
			int sLen = sArr != null ? sArr.length : 0;
			if (sLen == 0) {
				return new char[0];
			}
			int eLen = sLen / 3 * 3;
			int cCnt = (sLen - 1) / 3 + 1 << 2;
			int dLen = cCnt + (lineSep ? (cCnt - 1) / 76 << 1 : 0);
			char[] dArr = new char[dLen];

			int s = 0;
			int d = 0;
			for (int cc = 0; s < eLen;) {
				int i = (sArr[(s++)] & 0xFF) << 16 | (sArr[(s++)] & 0xFF) << 8 | sArr[(s++)] & 0xFF;

				dArr[(d++)] = CA[(i >>> 18 & 0x3F)];
				dArr[(d++)] = CA[(i >>> 12 & 0x3F)];
				dArr[(d++)] = CA[(i >>> 6 & 0x3F)];
				dArr[(d++)] = CA[(i & 0x3F)];
				if (lineSep) {
					cc++;
					if ((cc == 19) && (d < dLen - 2)) {
						dArr[(d++)] = '\r';
						dArr[(d++)] = '\n';
						cc = 0;
					}
				}
			}
			int left = sLen - eLen;
			if (left > 0) {
				int i = (sArr[eLen] & 0xFF) << 10 | (left == 2 ? (sArr[(sLen - 1)] & 0xFF) << 2 : 0);

				dArr[(dLen - 4)] = CA[(i >> 12)];
				dArr[(dLen - 3)] = CA[(i >>> 6 & 0x3F)];
				dArr[(dLen - 2)] = (left == 2 ? CA[(i & 0x3F)] : '=');
				dArr[(dLen - 1)] = '=';
			}
			return dArr;
		}

		private final String encodeToString(byte[] sArr, boolean lineSep) {
			return new String(encodeToChar(sArr, lineSep));
		}
	}

	@Override
	public void onInit(RTPDePacketizerMPEGTS rtDePacketizerMPEGTS, RTPContext rtpContext,
			RTPDePacketizerItem rtpDePacketizerItem) {
		this.debugLog = rtDePacketizerMPEGTS.getProperties()
				.getPropertyBoolean("rtpDePacketizerMPEGTSMonitorKLVDebugLog", this.debugLog);
	}

	@Override
	public void onStartup(RTPDePacketizerMPEGTS rtDePacketizerMPEGTS, RTPTrack rtpTrack) {
		WMSLoggerFactory.getLogger(CLASS).info(CLASSNAME + ".onStartup");

		this.rtDePacketizerMPEGTS = rtDePacketizerMPEGTS;
		this.rtpTrack = rtpTrack;

		RTPStream rtpStream = rtpTrack.getRTPStream();
		if (rtpStream != null)
			this.stream = rtpStream.getStream();
	}

	@Override
	public void onTimecodeReady(RTPDePacketizerMPEGTS rtDePacketizerMPEGTS) {
		WMSLoggerFactory.getLogger(CLASS).info(CLASSNAME + ".onTimecodeReady");

		this.isTimecodeReady = true;
	}

	@Override
	public void onPMT(RTPDePacketizerMPEGTS rtDePacketizerMPEGTS, ProgramMapTable newPMT) {
		WMSLoggerFactory.getLogger(CLASS).info(CLASSNAME + ".onPMT");

		boolean SCTE35RegDescFound = false;

		ArrayList<Descriptor> regDescriptors = newPMT.programDescriptors.get(Descriptor.DESCRIPTOR_TAG_REGISTRATION);

		if (regDescriptors != null && regDescriptors.size() > 0) {
			for (Descriptor desc : regDescriptors) {
				SCTE35RegDescFound |= ((RegistrationDescriptor) desc).formatIdentifier == RegistrationDescriptor.REG_IDENTIFICATION_SCTE_SPLICE_FORMAT;
			}
		}

		for (StreamInfo s : newPMT.streams.values()) {
			if (SCTE35RegDescFound) {
				ArrayList<Descriptor> descriptors = null;

				if (descriptors == null)
					descriptors = s.descriptors.get(Descriptor.DESCRIPTOR_TAG_CUE_IDENTIFIER);

				if (descriptors == null)
					descriptors = s.descriptors.get(Descriptor.DESCRIPTOR_TAG_STREAM_IDENTIFIER);

				if (descriptors != null) {
					WMSLoggerFactory.getLogger(CLASS)
							.info(CLASSNAME + ".onPMT: Hit cue point PID: 0x" + Integer.toHexString(s.PID));

					if (!rtDePacketizerMPEGTS.containsPIDMonitorMap(s.PID)) {
						rtDePacketizerMPEGTS.putPIDMonitorMap(s.PID, new MPEGTSMonitorCUE());
					}
				}
			}
		}
	}
}
