
Wowza version supported : 4.7.0 and above

Steps to follow: 
----------------
1) Copy SCTEEnhanced.jar to /usr/local/WowzaStreamingEngine/lib

2) Create a Live Application with name as "scte_enhanced_app", only hls in playback type

3) Set properties in Custom properties RTP section for rtpDePacketizerMPEGTSMonitorCUEDebugLog and 
   rtpDePacketizerMPEGTSListenerClass

		<Properties>
			<Property>
				<Name>rtpDePacketizerMPEGTSMonitorCUEDebugLog</Name>
				<Value>true</Value>
				<Type>Boolean</Type>
			</Property>
			<Property>
				<Name>rtpDePacketizerMPEGTSListenerClass</Name>
				<Value>com.amagi.thunderstorm.wowza.RTPDePacketizerMPEGTSMonitorCUE</Value>
			</Property>
		</Properties>

4) Set Live stream Packatizer properties as below 

		<LiveStreamPacketizer>
		<Properties>
		<Property>
			<Name>cupertinoMaxChunkCount</Name>
			<Value>50</Value>
			<Type>Integer</Type>
		</Property>
		<Property>
			<Name>cupertinoChunkDurationTarget</Name>
			<Value>5000</Value>
			<Type>Integer</Type>
		</Property>
		<Property>
			<Name>cupertinoPlaylistChunkCount</Name>
			<Value>8</Value>
			<Type>Integer</Type>
		</Property>
		<Property>
			<Name>cupertinoEnableDataEvents</Name>
			<Value>true</Value>
			<Type>Boolean</Type>
		</Property>
		<Property>
			<Name>cupertinoMPEGTSDataPID</Name>
			<Value>-1</Value>
			<Type>Integer</Type>
		</Property>
		<Property>
			<Name>cupertinoCalculateChunkIDBasedOnTimecode</Name>
			<Value>true</Value>
			<Type>Boolean</Type>
		</Property>
		</Properties>
		</LiveStreamPacketizer>

5) Add modules below

		<Module>
			<Name>SCTEEnhanced</Name>
			<Description>Detect SCTE nD chunk on Advert Boundary</Description>
			<Class>com.amagi.thunderstorm.wowza.SCTEEnhanced</Class>
		</Module>
		<Module>
			<Name>ModulePushPublish</Name>
			<Description>ModulePushPublish</Description>
			<Class>com.wowza.wms.pushpublish.module.ModulePushPublish</Class>
		</Module>
6) Add a custom property for HTTP Streamer as below.

		<HTTPStreamer>
			<Properties>
				<Property>
					<Name>cupertinoEnableProgramDateTime</Name>
					<Value>true</Value>
					<Type>Boolean</Type>
				</Property>
			</Properties>
		</HTTPStreamer>


7) Add PUSH Publish map property as below

		<Property>
  		  <Name>pushPublishMapPath</Name>
		<Value>${com.wowza.wms.context.VHostConfigHome}/conf/${com.wowza.wms.context.Application}/PushPublishMap.txt</Value>
		<Type>String</Type>
		</Property>

8) Create PushPublisMap.txt in conf/scte_enhanced_app folder
   - Use file with same name in settings folder as reference
   - replace "file.root":"/home/amagi/local-file-cache" to reflect your location where
     you want to keep local cache of TS segments /usr/share/nginx/html

9) Inside /usr/local/WowzaStreamingEngine/conf folder create PushPublishProfilesCustom.xml
   Copy content from file with same name in settings as reference

10) Install nginx 
   sudo apt-get update
   sudo apt-get install nginx

11) Copy files in nginx folder in settings to /etc/nginx folder
    - Change port and root path as in step 8 above

Typical commands :

sudo systemctl restart WowzaStreamingEngineManager.service
sudo systemctl restart WowzaStreamingEngine.service
sudo service nginx restart
