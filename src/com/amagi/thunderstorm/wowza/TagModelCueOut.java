package com.amagi.thunderstorm.wowza;
import java.io.*;
import com.wowza.wms.manifest.model.m3u8.tag.*;
import com.wowza.wms.manifest.writer.m3u8.*;

public class TagModelCueOut extends TagModel
{
	private static final int MILIS2SECOND = 1000;
	public TagModelCueOut(long Duration)
	{
		super(String.format("EXT-X-CUE-OUT:%.3f",((float) Duration/ MILIS2SECOND)));
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
		//writer.write(String.format("%s:%d", tagName,mDuration));
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
