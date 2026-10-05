import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.io.*;
import java.util.*;
import java.awt.image.BufferedImage;
import javax.imageio.ImageIO;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.XMLConstants;

/** A deliberately small boundary experiment, not a production upload validator. */
public class ImageLab {
    static final int MAX=10*1024*1024;
    static final byte[] SIGNATURE={(byte)137,80,78,71,13,10,26,10};
    static final String CSP="sandbox; default-src 'none'; img-src data:; style-src 'unsafe-inline'";
    static final String SVG="""
        <svg xmlns="http://www.w3.org/2000/svg" width="360" height="120">
          <rect id="box" width="360" height="120" fill="#b13c35"/>
          <text id="state" x="15" y="65" fill="white" font-size="24">UNCHANGED</text>
          <script>document.getElementById('box').setAttribute('fill','#217a49');document.getElementById('state').textContent='EXECUTED';</script>
        </svg>
        """;
    static int checks;
    static byte[] bytes(String s) {return s.getBytes(StandardCharsets.UTF_8);}
    static void check(String label, boolean condition) {
        if(!condition) throw new AssertionError(label);
        checks++; System.out.println("PASS "+label);
    }
    static boolean accepts(byte[] b,String mime) {
        if(b.length==0||b.length>MAX) return false;
        if("image/svg+xml".equals(mime)) return svg(b);
        return "image/png".equals(mime)&&b.length>=12&&Arrays.equals(Arrays.copyOf(b,8),SIGNATURE);
    }
    static boolean svg(byte[] b) {
        try {
            var f=DocumentBuilderFactory.newInstance();f.setNamespaceAware(true);
            f.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING,true);
            f.setFeature("http://apache.org/xml/features/disallow-doctype-decl",true);
            f.setFeature("http://xml.org/sax/features/external-general-entities",false);
            f.setFeature("http://xml.org/sax/features/external-parameter-entities",false);
            f.setXIncludeAware(false);f.setExpandEntityReferences(false);
            var p=f.newDocumentBuilder();p.setErrorHandler(new org.xml.sax.helpers.DefaultHandler(){
                @Override public void fatalError(org.xml.sax.SAXParseException e)throws org.xml.sax.SAXException{throw e;}
            });
            var root=p.parse(new ByteArrayInputStream(b)).getDocumentElement();
            return "svg".equals(root.getLocalName())&&"http://www.w3.org/2000/svg".equals(root.getNamespaceURI());
        } catch(Exception e){return false;}
    }
    static boolean decodes(byte[] b){
        try{return ImageIO.read(new ByteArrayInputStream(b))!=null;}catch(IOException e){return false;}
    }
    static byte[] png() throws IOException {
        var out=new ByteArrayOutputStream();ImageIO.write(new BufferedImage(2,2,BufferedImage.TYPE_INT_RGB),"png",out);return out.toByteArray();
    }
    static void verify() throws Exception {
        byte[] good=png(), truncated=Arrays.copyOf(SIGNATURE,12);
        check("valid-png gate=true decode=true",accepts(good,"image/png")&&decodes(good));
        check("mime-mismatch gate=false",!accepts(good,"image/jpeg"));
        check("signature-only gate=true decode=false",accepts(truncated,"image/png")&&!decodes(truncated));
        check("empty rejected",!accepts(new byte[0],"image/png"));
        check("over-10MiB rejected",!accepts(Arrays.copyOf(good,MAX+1),"image/png"));
        check("10MiB boundary gate=true",accepts(Arrays.copyOf(good,MAX),"image/png"));
        check("normal-svg accepted",accepts(bytes("<svg xmlns='http://www.w3.org/2000/svg'/>"),"image/svg+xml"));
        check("doctype rejected",!accepts(bytes("<!DOCTYPE svg [<!ENTITY example 'LOCAL'>]><svg xmlns='http://www.w3.org/2000/svg'>&example;</svg>"),"image/svg+xml"));
        check("wrong-namespace rejected",!accepts(bytes("<svg xmlns='urn:example'/>"),"image/svg+xml"));
        check("script-element survives XML acceptance",accepts(bytes(SVG),"image/svg+xml")&&SVG.contains("<script>"));
        System.out.println("ALL "+checks+" CHECKS PASSED; Java="+System.getProperty("java.version"));
    }
    static void serve(int port)throws Exception {
        var server=HttpServer.create(new InetSocketAddress("127.0.0.1",port),0);
        server.createContext("/",e->{
            String path=e.getRequestURI().getPath();boolean isSvg=path.equals("/plain.svg")||path.equals("/protected.svg");
            String body;
            if(isSvg){body=SVG;e.getResponseHeaders().set("Content-Type","image/svg+xml");if(path.equals("/protected.svg"))e.getResponseHeaders().set("Content-Security-Policy",CSP);}
            else if(path.equals("/")){
                body="""
                <!doctype html><html lang="en"><meta charset="utf-8"><title>Image boundary lab</title>
                <style>body{font:18px system-ui;max-width:850px;margin:32px auto;color:#222}iframe,img{width:360px;height:120px;border:1px solid #bbb}h2{font-size:20px}code{background:#eee}</style>
                <h1>Same SVG, three delivery contexts</h1><p>The script only changes its own label and color. No external requests.</p>
                <h2>1. SVG document (nosniff only)</h2><iframe title="Plain SVG document" src="/plain.svg"></iframe>
                <h2>2. SVG document (nosniff + response CSP sandbox)</h2><iframe title="Protected SVG document" src="/protected.svg"></iframe>
                <h2>3. Image embedding (img, nosniff only)</h2><img alt="SVG image embedding" src="/plain.svg">
                <p>Prediction: which label becomes EXECUTED? Compare with Java's XML acceptance.</p>
                </html>
                """;e.getResponseHeaders().set("Content-Type","text/html; charset=utf-8");
            }else{e.sendResponseHeaders(404,-1);e.close();return;}
            e.getResponseHeaders().set("X-Content-Type-Options","nosniff");e.getResponseHeaders().set("Cache-Control","no-store");
            byte[] payload=bytes(body);e.sendResponseHeaders(200,payload.length);try(var out=e.getResponseBody()){out.write(payload);}
        });
        Runtime.getRuntime().addShutdownHook(new Thread(()->server.stop(0)));server.start();
        System.out.println("BROWSER_LAB http://127.0.0.1:"+server.getAddress().getPort()+"/ ; Ctrl-C stops this local server.");
    }
    public static void main(String[] args)throws Exception {
        String mode=args.length==0?"guided":args[0];
        if(!Set.of("guided","verify","serve").contains(mode))throw new IllegalArgumentException("guided | verify | serve [port]");
        if(mode.equals("guided")){
            System.out.println("예상: PNG 시그니처만 있으면 디코딩될까요? XML 파싱 성공은 SVG script 제거를 뜻할까요? Enter 실행, q 종료.");
            if("q".equalsIgnoreCase(new BufferedReader(new InputStreamReader(System.in)).readLine()))return;
        }
        verify();
        if(mode.equals("serve"))serve(args.length>1?Integer.parseInt(args[1]):0);
        else System.out.println("브라우저 비교: ./play-image-01 serve 후 출력 URL 열기. 답은 ANSWERS.md에서 확인하세요.");
    }
}
