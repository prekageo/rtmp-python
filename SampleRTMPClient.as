package {
	import flash.display.Sprite;
	import flash.text.TextField;
    import flash.net.NetConnection;
    import flash.net.SharedObject;
    import flash.net.ObjectEncoding;
    import flash.events.*;
	
	public class SampleRTMPClient extends Sprite {
		internal var display_txt:TextField;
        internal var netconn:NetConnection;
        internal var so:SharedObject;
        internal var so2:SharedObject;
        
		public function SampleRTMPClient() {
			display_txt = new TextField();
			display_txt.text = "Connecting...";
            display_txt.width = 400;
            display_txt.height = 300;
            display_txt.border = true;
            display_txt.multiline = true;
            display_txt.wordWrap = true;
			addChild(display_txt);
            
            NetConnection.defaultObjectEncoding = ObjectEncoding.AMF0;
            netconn = new NetConnection();
            netconn.addEventListener(NetStatusEvent.NET_STATUS, netStatusHandler);

            netconn.connect("rtmp://127.0.0.1:80/test","arg1",42);
		}
        
        public function netStatusHandler(event:NetStatusEvent):void{
            switch(event.info.code){
                case 'NetConnection.Connect.Failed':
                case 'NetConnection.Connect.Rejected':
                    display_txt.text = "Connection failed.";
                    break;
                case 'NetConnection.Connect.Success':
                    display_txt.text = "Connection OK.";
                    so = SharedObject.getRemote("so_name", netconn.uri);
                    so.addEventListener(SyncEvent.SYNC, soSyncHandler);
                    so.connect(netconn);
                    break;
            }
        }

        public function soSyncHandler(event:SyncEvent):void{
            display_txt.text = "so_name.sparam = " + so.data.sparam;
            so2 = SharedObject.getRemote("so2_name", netconn.uri);
            so2.addEventListener(SyncEvent.SYNC, so2SyncHandler);
            so2.connect(netconn);
        }
        
        public function so2SyncHandler(event:SyncEvent):void{
            display_txt.text = "so2_name.sparam = " + so2.data.sparam;
        }
	}
}
