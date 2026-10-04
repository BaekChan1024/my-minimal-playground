package playground.security;

import com.nimbusds.jose.*;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.*;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jwt.*;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.context.annotation.*;
import org.springframework.mock.web.MockServletContext;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.SpringSecurityCoreVersion;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.core.*;
import org.springframework.security.oauth2.jwt.*;
import org.springframework.security.oauth2.server.resource.authentication.*;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.context.support.AnnotationConfigWebApplicationContext;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity;

public class JwtLab {
    static final Instant NOW=Instant.parse("2030-01-01T00:00:00Z");
    static final String ISSUER="https://issuer.example.test", AUDIENCE="content-lab";
    static final AtomicInteger decodeCalls=new AtomicInteger(), jwksCalls=new AtomicInteger();
    static JwtDecoder countedDecoder;
    static void require(boolean value,String message) { if(!value)throw new IllegalStateException(message); }

    @Configuration @EnableWebSecurity @EnableWebMvc
    static class Config {
        @Bean JwtDecoder decoder(){return countedDecoder;}
        @Bean Endpoint endpoint(){return new Endpoint();}
        @Bean SecurityFilterChain chain(HttpSecurity http) throws Exception {
            var converter=new JwtAuthenticationConverter();
            var scopes=new JwtGrantedAuthoritiesConverter();
            converter.setJwtGrantedAuthoritiesConverter(token->{
                Collection<GrantedAuthority> authorities=new ArrayList<>(Objects.requireNonNull(scopes.convert(token)));
                Object realm=token.getClaim("realm_access");
                if(realm instanceof Map<?,?> access && access.get("roles") instanceof Collection<?> roles)
                    roles.stream().filter(String.class::isInstance).map(String.class::cast)
                        .map(role->new SimpleGrantedAuthority("ROLE_"+role)).forEach(authorities::add);
                return authorities;
            });
            return http.authorizeHttpRequests(a->a.requestMatchers("/write-check")
                .hasAnyAuthority("SCOPE_content:write","ROLE_content-author").anyRequest().denyAll())
                .oauth2ResourceServer(o->o.jwt(j->j.jwtAuthenticationConverter(converter)))
                .sessionManagement(s->s.sessionCreationPolicy(SessionCreationPolicy.STATELESS)).build();
        }
    }
    @RestController static class Endpoint {
        @GetMapping("/write-check") String check(){return "authorized";}
    }
    static NimbusJwtDecoder decoder(String uri,boolean audience) {
        var d=NimbusJwtDecoder.withJwkSetUri(uri).build();
        var time=new JwtTimestampValidator(Duration.ofSeconds(30));
        time.setClock(Clock.fixed(NOW,ZoneOffset.UTC));
        OAuth2TokenValidator<Jwt> base=new DelegatingOAuth2TokenValidator<>(time,new JwtIssuerValidator(ISSUER),JwtTypeValidator.jwt());
        OAuth2TokenValidator<Jwt> aud=new JwtClaimValidator<List<String>>("aud", a->a!=null&&a.contains(AUDIENCE));
        d.setJwtValidator(audience?new DelegatingOAuth2TokenValidator<>(base,aud):base);
        return d;
    }
    static String token(RSAKey key,String issuer,String audience,Instant exp,String scope,List<String> roles) throws Exception {
        var claims=new JWTClaimsSet.Builder().issuer(issuer).subject("synthetic-reader")
            .audience(audience).issueTime(Date.from(NOW.minusSeconds(3600)))
            .notBeforeTime(Date.from(NOW.minusSeconds(3600))).expirationTime(Date.from(exp))
            .claim("scope",scope).claim("realm_access",Map.of("roles",roles)).build();
        var jwt=new SignedJWT(new JWSHeader.Builder(JWSAlgorithm.RS256).type(JOSEObjectType.JWT).keyID("lab-key").build(),claims);
        jwt.sign(new RSASSASigner(key));return jwt.serialize();
    }
    static void request(MockMvc mvc,String name,MockHttpServletRequestBuilder request,int expected,int calls) throws Exception {
        int before=decodeCalls.get();var response=mvc.perform(request).andReturn().getResponse();
        require(response.getStatus()==expected,name+": unexpected HTTP status");
        require(decodeCalls.get()-before==calls,name+": unexpected decoder count");
        if(expected==200)require(response.getContentAsString().equals("authorized"),name+": endpoint not reached");
        System.out.printf("PASS %s status=%d decoderCalls=%d%n",name,expected,calls);
    }
    public static void main(String[] args) throws Exception {
        String mode=args.length==0?"guided":args[0];require(Set.of("guided","verify").contains(mode),"Use guided or verify");
        if(mode.equals("guided")){
            System.out.println("예상: jwt()로 200이면 실제 서명도 검증했을까요? 유효한 서명이면 권한도 있을까요? Enter 실행 / q 종료");
            if(new Scanner(System.in).nextLine().strip().equalsIgnoreCase("q"))return;
        }
        // Ephemeral synthetic keys only. Never log or persist private keys or serialized tokens.
        var trusted=new RSAKeyGenerator(2048).keyID("lab-key").generate();
        var wrong=new RSAKeyGenerator(2048).keyID("lab-key").generate();
        byte[] publicJwks=new JWKSet(trusted.toPublicJWK()).toString().getBytes(StandardCharsets.UTF_8);
        var server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
        server.createContext("/jwks",exchange->{jwksCalls.incrementAndGet();exchange.getResponseHeaders().set("Content-Type","application/json");exchange.sendResponseHeaders(200,publicJwks.length);try(var out=exchange.getResponseBody()){out.write(publicJwks);}});
        server.start();
        try(var context=new AnnotationConfigWebApplicationContext()) {
            String uri="http://127.0.0.1:"+server.getAddress().getPort()+"/jwks";
            var real=decoder(uri,true);countedDecoder=value->{decodeCalls.incrementAndGet();return real.decode(value);};
            context.setServletContext(new MockServletContext());context.register(Config.class);context.refresh();
            var mvc=MockMvcBuilders.webAppContextSetup(context).apply(springSecurity()).build();
            System.out.println("SpringSecurity="+SpringSecurityCoreVersion.getVersion()+" Spring="+org.springframework.core.SpringVersion.getVersion());
            request(mvc,"mock-writer",get("/write-check").with(jwt().jwt(j->j.issuer("untrusted-example")).authorities(new SimpleGrantedAuthority("SCOPE_content:write"))),200,0);
            require(jwksCalls.get()==0,"Mock must not load JWKS");
            request(mvc,"missing-token",get("/write-check"),401,0);
            String valid=token(trusted,ISSUER,AUDIENCE,NOW.plusSeconds(600),"content:write",List.of());
            request(mvc,"signed-writer",get("/write-check").header("Authorization","Bearer "+valid),200,1);
            require(jwksCalls.get()>0,"Real verification must load public JWKS");
            request(mvc,"wrong-signature",get("/write-check").header("Authorization","Bearer "+token(wrong,ISSUER,AUDIENCE,NOW.plusSeconds(600),"content:write",List.of())),401,1);
            request(mvc,"wrong-issuer",get("/write-check").header("Authorization","Bearer "+token(trusted,"https://other.example.test",AUDIENCE,NOW.plusSeconds(600),"content:write",List.of())),401,1);
            request(mvc,"expired",get("/write-check").header("Authorization","Bearer "+token(trusted,ISSUER,AUDIENCE,NOW.minusSeconds(600),"content:write",List.of())),401,1);
            String otherAudience=token(trusted,ISSUER,"other-service",NOW.plusSeconds(600),"content:write",List.of());
            require(decoder(uri,false).decode(otherAudience).getAudience().contains("other-service"),"Audience control fixture");
            System.out.println("PASS issuer-time-only otherAudienceAccepted=true");
            request(mvc,"wrong-audience",get("/write-check").header("Authorization","Bearer "+otherAudience),401,1);
            request(mvc,"valid-reader",get("/write-check").header("Authorization","Bearer "+token(trusted,ISSUER,AUDIENCE,NOW.plusSeconds(600),"content:read",List.of())),403,1);
            request(mvc,"realm-author",get("/write-check").header("Authorization","Bearer "+token(trusted,ISSUER,AUDIENCE,NOW.plusSeconds(600),"",List.of("content-author"))),200,1);
            System.out.println("ALL 10 CHECKS PASSED; mock bypass, real validation and authorization boundaries observed.");
        } finally {server.stop(0);}
    }
}
