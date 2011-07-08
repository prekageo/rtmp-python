""" Sample implementation of an RTMP client. """

import pyamf.util
import rtmp_protocol_base
import rtmp_protocol
import socket

def handshake(sock_stream):
    """ Perform the handshake sequence with the server. """
    sock_stream.write_uchar(3)
    c1 = rtmp_protocol_base.Packet(first=0,second=0,payload='x'*1528)
    c1.encode(sock_stream)
    sock_stream.flush()

    sock_stream.read_uchar()
    s1 = rtmp_protocol_base.Packet()
    s1.decode(sock_stream)
    
    c2 = rtmp_protocol_base.Packet(first=s1.first,second=s1.second, 
        payload=s1.payload)
    c2.encode(sock_stream)
    sock_stream.flush()
    
    s2 = rtmp_protocol_base.Packet()
    s2.decode(sock_stream)

def connect(reader, writer):
    """ Initiate a NetConnection with a Flash Media Server. """ 
    writer.write({
        'msg': rtmp_protocol.DataTypes.COMMAND,
        'command': [
            u'connect',
            1,
            {
                'videoCodecs': 252,
                'audioCodecs': 3191,
                'flashVer': u'WIN 10,1,85,3',
                'app': u'',
                'tcUrl': u'',
                'videoFunction': 1,
                'capabilities': 239,
                'pageUrl': u'',
                'fpad': False,
                'swfUrl': u'',
                'objectEncoding': 0
            }
        ]
    })
    writer.flush()
    
    while True:
        msg = reader.next()
        if msg['msg'] == rtmp_protocol.DataTypes.COMMAND and \
            msg['command'][0] == '_result' and msg['command'][1] == 1:
            assert(msg['command'][3]['code'] == 'NetConnection.Connect.Success')
            break

def main():
    """
    Start the client, connect to 127.0.0.1:80 and use 2 remote flash shared
    objects.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(('127.0.0.1', 80))
    sock_stream = rtmp_protocol.FileDataTypeMixIn(s.makefile())

    handshake(sock_stream)
    print 'INFO: Handshake OK'
    
    reader = rtmp_protocol.RtmpReader(sock_stream)
    writer = rtmp_protocol.RtmpWriter(sock_stream)
    connect(reader, writer)
    print 'INFO: Connected OK'
    
    so_name = rtmp_protocol.FlashSharedObject('so_name')
    so_name.use(reader, writer)

    so2_name = rtmp_protocol.FlashSharedObject('so2_name')
    so2_name.use(reader, writer)

    while True:
        packet = reader.next()
        if so_name.handle_packet(packet):
            print 'so_name.sparam = "%s"' % (so_name.data['sparam'],)
        elif so2_name.handle_packet(packet):
            print 'so2_name.sparam = "%s"' % (so2_name.data['sparam'],)
        else:
            print packet

if __name__ == '__main__':
    main()
