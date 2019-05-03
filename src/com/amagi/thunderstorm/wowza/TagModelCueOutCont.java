package com.amagi.thunderstorm.wowza;

import java.io.*;
import com.wowza.wms.manifest.model.m3u8.tag.*;
import com.wowza.wms.manifest.writer.m3u8.*;

public class TagModelCueOutCont extends TagModel {

	public TagModelCueOutCont(float durationElapsed,float totalDuration,String uSCTEInput) {
		super(String.format("EXT-X-CUE-OUT-CONT:ElapsedTime=%.3f,Duration=%d,SCTE35=%s",durationElapsed,
															(int)totalDuration,uSCTEInput));
		}

	@Override
	public boolean isMediaSegmentTag() {
		return false;
	}

	@Override
	public boolean isValid(Integer version) {

		return true;
	}

	@Override
	public String toString() {
		return "#" + tagName;
	}

	@Override
	public boolean validForMasterPlaylist() {
		
		return false;
	}

	@Override
	public boolean validForMediaPlaylist() {
		
		return true;
	}

	@Override
	public void write(TagWriter writer) throws IOException {
		writer.writeTag(tagName);
	}

}
