package com.lxpeak.lxpeakdb.transport;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

/*
* 为了避免特殊字符造成问题，这里会将数据转成十六进制字符串（Hex String），并为信息末尾加上换行符。
* 这样在发送和接收数据时，就可以很简单地使用 BufferedReader 和 Writer 来直接按行读写了。
*
* Q：为什么使用十六进制：
* A：（1）网络传输中使用 BufferedReader.readLine() 时，默认以换行符 \n 或 \r 作为行结束符。
*        若原始二进制数据中包含这些控制字符，readLine() 会提前截断数据，导致接收不完整。
*        转换为十六进制后，所有字节都被编码为 0-9 和 a-f 的普通字符，避开了控制字符的干扰。
*    （2）直接以字符串形式传输二进制数据时，不同平台的字符编码（如 UTF-8、GBK）可能导致字节解析错误。
*        十六进制编码将每个字节固定转换为 2 个 ASCII 字符（如 0x0A -> "0a"），完全规避了编码歧义。
*     具体示例：
*        假设原始字节数据为 [0x48, 0x0A, 0x65]（包含换行符 0x0A）：
*        直接传输：接收方 readLine() 会在 0x0A 处截断，只能读到 [0x48]，剩余 0x65 会被当作下一行。
*        Hex 编码后：数据变为 "480a65\n"，readLine() 会完整读取整行 "480a65"，解码后还原原始 3 个字节。
* */
public class Transporter {
    private Socket socket;
    private BufferedReader reader;
    private BufferedWriter writer;

    public Transporter(Socket socket) throws IOException {
        this.socket = socket;
        this.reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        this.writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
    }


    public void send(byte[] data) throws Exception {
        String raw = hexEncode(data);
        writer.write(raw);
        writer.flush();
    }

    public byte[] receive() throws Exception {
        String line = reader.readLine();
        if(line == null) {
            close();
        }
        return hexDecode(line);
    }

    public void close() throws IOException {
        writer.close();
        reader.close();
        socket.close();
    }

    private String hexEncode(byte[] buf) {
        return Hex.encodeHexString(buf, true)+"\n";
    }

    private byte[] hexDecode(String buf) throws DecoderException {
        return Hex.decodeHex(buf);
    }
}
