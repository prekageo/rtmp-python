"""
Provides classes for creating RTMP (Real Time Message Protocol) servers and
clients.
"""

import pyamf.amf0
import pyamf.util
import rtmp_protocol_base

class FileDataTypeMixIn(pyamf.util.DataTypeMixIn):
    """
    Provides a wrapper for a file object that enables reading and writing of raw
    data types for the file.
    """

    def __init__(self, fileobject):
        self.fileobject = fileobject
        pyamf.util.DataTypeMixIn.__init__(self)
    
    def read(self, length):
        return self.fileobject.read(length)
    
    def write(self, data):
        self.fileobject.write(data)

    def flush(self):
        self.fileobject.flush()

    def at_eof(self):
        return False

class DataTypes:
    """ Represents an enumeration of the RTMP message datatypes. """
    USER_CONTROL = 4
    WINDOW_ACK_SIZE = 5
    SET_PEER_BANDWIDTH = 6
    SHARED_OBJECT = 19
    COMMAND = 20

class SOEventTypes:
    """ Represents an enumeration of the shared object event types. """
    USE = 1
    RELEASE = 2
    CHANGE = 4
    MESSAGE = 6
    CLEAR = 8
    DELETE = 9
    USE_SUCCESS = 11

class RtmpReader:
    """ This class reads RTMP messages from a stream. """

    chunk_size = 128

    def __init__(self, stream):
        """
        Initialize the RTMP reader and set it to read from the specified stream.
        """
        self.stream = stream

    def __iter__(self):
        return self

    def next(self):
        """ Read one RTMP message from the stream and return it. """
        if self.stream.at_eof():
            raise StopIteration

        # Read the message into body_stream. The message may span a number of
        # chunks (each one with its own header).
        message_body = []
        msg_body_len = 0
        header = rtmp_protocol_base.header_decode(self.stream)
        while True:
            read_bytes = min(header.bodyLength - msg_body_len, self.chunk_size)
            message_body.append(self.stream.read(read_bytes))
            msg_body_len += read_bytes
            if msg_body_len >= header.bodyLength:
                break
            next_header = rtmp_protocol_base.header_decode(self.stream)
            # WORKAROUND: even though the RTMP specification states that the
            # extended timestamp field DOES NOT follow type 3 chunks, it seems
            # that Flash player 10.1.85.3 and Flash Media Server 3.0.2.217 send
            # and expect this field here.
            if header.timestamp >= 0x00ffffff:
                self.stream.read_ulong()
            assert(next_header.streamId == -1)
            assert(next_header.datatype == -1)
            assert(next_header.timestamp == -1)
            assert(next_header.bodyLength == -1)
        assert(header.bodyLength == msg_body_len)
        body_stream = pyamf.util.BufferedByteStream(''.join(message_body))
        
        # Decode the message based on the datatype present in the header
        ret = {'msg':header.datatype}
        if ret['msg'] == DataTypes.USER_CONTROL:
            ret['event_type'] = body_stream.read_ushort()
            ret['event_data'] = body_stream.read()
        elif ret['msg'] == DataTypes.WINDOW_ACK_SIZE:
            ret['window_ack_size'] = body_stream.read_ulong()
        elif ret['msg'] == DataTypes.SET_PEER_BANDWIDTH:
            ret['window_ack_size'] = body_stream.read_ulong()
            ret['limit_type'] = body_stream.read_uchar()
        elif ret['msg'] == DataTypes.SHARED_OBJECT:
            decoder = pyamf.amf0.Decoder(body_stream)
            obj_name = decoder.readString()
            curr_version = body_stream.read_ulong()
            flags = body_stream.read(8)

            # A shared object message may contain a number of events.
            events = []
            while not body_stream.at_eof():
                event = self.read_shared_object_event(body_stream, decoder)
                events.append(event)

            ret['obj_name'] = obj_name
            ret['curr_version'] = curr_version
            ret['flags'] = flags
            ret['events'] = events
        elif ret['msg'] == DataTypes.COMMAND:
            decoder = pyamf.amf0.Decoder(body_stream)
            commands = []
            while not body_stream.at_eof():
                commands.append(decoder.readElement())
            ret['command'] = commands
        else:
            print 'ERROR: unknown message type %d' % (ret['msg'],)
            assert(False)

        return ret

    def read_shared_object_event(self, body_stream, decoder):
        """
        Helper method that reads one shared object event found inside a shared
        object RTMP message.
        """
        so_body_type = body_stream.read_uchar()
        so_body_size = body_stream.read_ulong()

        event = {'type':so_body_type}
        if event['type'] == SOEventTypes.USE:
            assert(so_body_size == 0)
            event['data'] = ''
        elif event['type'] == SOEventTypes.RELEASE:
            assert(so_body_size == 0)
            event['data'] = ''
        elif event['type'] == SOEventTypes.CHANGE:
            start_pos = body_stream.tell()
            changes = {}
            while body_stream.tell() < start_pos + so_body_size:
                attrib_name = decoder.readString()
                attrib_value = decoder.readElement()
                assert(attrib_name not in changes)
                changes[attrib_name] = attrib_value
            assert(body_stream.tell() == start_pos + so_body_size)
            event['data'] = changes
        elif event['type'] == SOEventTypes.MESSAGE:
            start_pos = body_stream.tell()
            msg_params = []
            while body_stream.tell() < start_pos + so_body_size:
                msg_params.append(decoder.readElement())
            assert(body_stream.tell() == start_pos + so_body_size)
            event['data'] = msg_params
        elif event['type'] == SOEventTypes.CLEAR:
            assert(so_body_size == 0)
            event['data'] = ''
        elif event['type'] == SOEventTypes.DELETE:
            event['data'] = decoder.readString()
        elif event['type'] == SOEventTypes.USE_SUCCESS:
            assert(so_body_size == 0)
            event['data'] = ''
        else:
            print 'ERROR: unknown SO body type %d' % (event['type'],)
            assert(False)
        
        return event
    
class RtmpWriter:
    """ This class writes RTMP messages into a stream. """

    chunk_size = 128

    def __init__(self, stream):
        """
        Initialize the RTMP writer and set it to write into the specified
        stream.
        """
        self.stream = stream
    
    def flush(self):
        """ Flush the underlying stream. """
        self.stream.flush()

    def write(self, message):
        """ Encode and write the specified message into the stream. """
        datatype = message['msg'] 
        body_stream = pyamf.util.BufferedByteStream()
        encoder = pyamf.amf0.Encoder(body_stream)

        if datatype == DataTypes.USER_CONTROL:
            body_stream.write_ushort(message['event_type'])
            body_stream.write(message['event_data'])
        elif datatype == DataTypes.WINDOW_ACK_SIZE:
            body_stream.write_ulong(message['window_ack_size'])
        elif datatype == DataTypes.SET_PEER_BANDWIDTH:
            body_stream.write_ulong(message['window_ack_size'])
            body_stream.write_uchar(message['limit_type'])
        elif datatype == DataTypes.COMMAND:
            for command in message['command']:
                encoder.writeElement(command)
        elif datatype == DataTypes.SHARED_OBJECT:
            encoder.writeString(message['obj_name'],writeType=False)
            body_stream.write_ulong(message['curr_version'])
            body_stream.write(message['flags'])
            
            for event in message['events']:
                self.write_shared_object_event(event, body_stream)
        else:
            print 'ERROR: unknown message type %d' % (datatype,)
            assert(False)

        self.send_msg(datatype, body_stream.getvalue())
    
    def write_shared_object_event(self, event, body_stream):
        """
        Helper method that writes one shared object inside a shared object RTMP
        message.
        """
        
        inner_stream = pyamf.util.BufferedByteStream()
        encoder = pyamf.amf0.Encoder(inner_stream)

        event_type = event['type']
        if event_type == SOEventTypes.USE:
            assert(event['data'] == '')
        elif event_type == SOEventTypes.CHANGE:
            for attrib_name in event['data']:
                attrib_value = event['data'][attrib_name]
                encoder.writeString(attrib_name,writeType=False)
                encoder.writeElement(attrib_value)
        elif event['type'] == SOEventTypes.CLEAR:
            assert(event['data'] == '')
        elif event['type'] == SOEventTypes.USE_SUCCESS:
            assert(event['data'] == '')
        else:
            print 'ERROR: unknown SO body type %d' % (event_type,)
            assert(False)

        body_stream.write_uchar(event_type)
        body_stream.write_ulong(len(inner_stream))
        body_stream.write(inner_stream.getvalue())

    def send_msg(self, datatype, body):
        """
        Helper method that send the specified message into the stream. Takes
        care to prepend the necessary headers and split the message into
        appropriately sized chunks.
        """
        
        # Values that just work. :-)
        if datatype >= 1 and datatype <= 7:
            channel_id = 2
            stream_id = 0
        else:
            channel_id = 3
            stream_id = 0
        timestamp = 0

        header = rtmp_protocol_base.Header(
            channelId=channel_id,
            streamId=stream_id,
            datatype=datatype,
            bodyLength=len(body),
            timestamp=timestamp)
        rtmp_protocol_base.header_encode(self.stream, header)
        
        for i in xrange(0,len(body),self.chunk_size):
            chunk = body[i:i+self.chunk_size]
            self.stream.write(chunk)
            if i+self.chunk_size < len(body):
                rtmp_protocol_base.header_encode(self.stream, header, header)

class FlashSharedObject:
    """
    This class represents a Flash Remote Shared Object. Its data are located
    inside the self.data dictionary.
    """

    def __init__(self, name):
        """
        Initialize a new Flash Remote SO with a given name and empty data.
        """
        self.name = name
        self.data = {}
        self.use_success = False

    def use(self, reader, writer):
        """
        Initialize usage of the SO by contacting the Flash Media Server. Any
        remote changes to the SO should be now propagated to the client.
        """
        self.use_success = False

        msg = {
            'msg': DataTypes.SHARED_OBJECT,
            'curr_version': 0,
            'flags': '\x00\x00\x00\x00\x00\x00\x00\x00',
            'events': [
                {
                    'data': '',
                    'type': SOEventTypes.USE
                }
            ],
            'obj_name': self.name
        }
        writer.write(msg)
        writer.flush()
    
    def handle_message(self, message):
        """
        Handle an incoming RTMP message. Check if it is of any relevance for the
        specific SO and process it, otherwise ignore it.
        """
        if message['msg'] == DataTypes.SHARED_OBJECT and \
            message['obj_name'] == self.name:
            events = message['events']

            if not self.use_success:
                assert(events[0]['type'] == SOEventTypes.USE_SUCCESS)
                assert(events[1]['type'] == SOEventTypes.CLEAR)
                events = events[2:]
                self.use_success = True

            self.handle_events(events)
            return True
        else:
            return False
    
    def handle_events(self, events):
        """ Handle SO events that target the specific SO. """
        for event in events:
            event_type = event['type']
            if event_type == SOEventTypes.CHANGE:
                for key in event['data']:
                    self.data[key] = event['data'][key]
            elif event_type == SOEventTypes.DELETE:
                key = event['data']
                assert(key in self.data)
                del self.data[key]
            else:
                print 'ERROR: unknown event %d' % (event_type,)
                assert(False)
