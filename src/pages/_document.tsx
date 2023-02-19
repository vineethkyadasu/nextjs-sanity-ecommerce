import Document, { Html, Head, Main, NextScript } from "next/document";

class MyDocument extends Document {
  render() {
    return (
      <Html lang="en">
        <Head>
          <link
            rel="preconnect"
            href="https://fonts.gstatic.com"
            crossOrigin="true"
          />
        </Head>
        <body>
        <noscript><iframe src="https://www.googletagmanager.com/ns.html?id=GTM-W8CSNDP"
height="0" width="0" style="display:none;visibility:hidden"></iframe></noscript>
          <Main />
          <NextScript />
        </body>
      </Html>
    );
  }
}

export default MyDocument;
