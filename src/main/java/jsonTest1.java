//import com.fasterxml.jackson.core.JsonParseException;
//import com.fasterxml.jackson.core.JsonParser;
//import com.fasterxml.jackson.core.JsonProcessingException;
//import com.fasterxml.jackson.databind.DeserializationContext;
//import com.fasterxml.jackson.databind.DeserializationFeature;
//import com.fasterxml.jackson.databind.JsonDeserializer;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
//import com.google.gson.internal.$Gson$Preconditions;
//import sun.applet.Main;
//
//import java.awt.print.Book;
//import java.awt.print.PageFormat;
//import java.io.IOException;
//import java.io.InputStream;
//import java.math.BigInteger;
//
///**
// * Copyright (c) 2020-2030 All right Reserved
// *
// * @author :   Andy Xu
// * @version :   1.0
// * @date :   8/20/2020 10:26 PM
// */
//public class jsonTest1 {
//    public static void main(String[] args) throws IOException {
//        InputStream input = Main.class.getResourceAsStream("E:\\workspace\\FlinkExercise\\src\\main\\resources\\test.json");
//        ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());
//        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
//        Book book = mapper.readValue(input, Book.class);
//        PageFormat pageFormat = book.getPageFormat(3);
//        System.out.println(pageFormat);
//        String json = mapper.writeValueAsString(book);
//    }
//
//    public class IsbnDeserializer extends JsonDeserializer<BigInteger> {
//        public BigInteger deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
//            // 读取原始的JSON字符串内容:
//            String s = p.getValueAsString();
//            if (s != null) {
//                try {
//                    return new BigInteger(s.replace("-", ""));
//                } catch (NumberFormatException e) {
//                    throw new JsonParseException(p, s, e);
//                }
//            }
//            return null;
//        }
//    }
//}
