import org.apache.coyote.http2.Http2Protocol
import org.eclipse.jetty.http2.server.HTTP2CServerConnectionFactory
import org.springframework.boot.web.embedded.jetty.JettyServerCustomizer
import org.springframework.boot.web.embedded.jetty.JettyServletWebServerFactory
import org.springframework.boot.web.embedded.tomcat.TomcatConnectorCustomizer
import org.springframework.context.annotation.Bean

@Bean // это для Jetty
fun jettyServerCustomizer(): JettyServletWebServerFactory {
    val jettyServletWebServerFactory = JettyServletWebServerFactory()

    val c = JettyServerCustomizer {
        (it.connectors[0].getConnectionFactory("h2c") as HTTP2CServerConnectionFactory).maxConcurrentStreams = 1_000_000
    }

    jettyServletWebServerFactory.serverCustomizers.add(c)
    return jettyServletWebServerFactory
}

@Bean
fun tomcatConnectorCustomizer(): TomcatConnectorCustomizer {
    return TomcatConnectorCustomizer {
        try {
            (it.protocolHandler.findUpgradeProtocols().get(0) as Http2Protocol).maxConcurrentStreams = 10_000_000
        } catch (e: Exception) {

        }
    }
}
