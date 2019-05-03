package com.amagi.thunderstorm.wowza;
import java.io.*;

import com.wowza.wms.manifest.model.m3u8.tag.*;
import com.wowza.wms.manifest.writer.m3u8.*;

public class TagModelCueIn extends TagModel
{
	public TagModelCueIn()
	{
		super("EXT-X-CUE-IN");
	}

	@Override
	public boolean validForMasterPlaylist()
	{
		return false;
	}

	@Override
	public boolean validForMediaPlaylist()
	{
		return true;
	}

	@Override
	public String toString()
	{
		return "#" + tagName;
	}

	@Override
	public void write(TagWriter writer) throws IOException
	{
		writer.writeTag(tagName);
	}

	@Override
	public boolean isMediaSegmentTag()
	{
		return false;
	}

	@Override
	public boolean isValid(Integer version)
	{
		return true;
	}
}
