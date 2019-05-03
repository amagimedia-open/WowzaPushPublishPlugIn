package com.amagi.thunderstorm.wowza;

import java.io.*;
import com.wowza.wms.manifest.model.m3u8.tag.*;
import com.wowza.wms.manifest.writer.m3u8.*;

public class TagModelOATCLSSCTE35 extends TagModel {
	public TagModelOATCLSSCTE35(String uSCTEInput) {
		super(String.format("EXT-OATCLS-SCTE35:%s",uSCTEInput));
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
