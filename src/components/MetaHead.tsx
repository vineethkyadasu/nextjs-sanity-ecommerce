import Head from "next/head";
import { useRouter } from "next/router";

interface MetaHeadProps {
  title?: string;
  description: string;
  featuredImage?: string;
  type?: string;
}

const siteUrl = "https://react-woocommerce.vercel.app";

const MetaHead: React.FC<MetaHeadProps> = ({
  title,
  description,
  featuredImage,
  type = "article"
}) => {
  const Router = useRouter();

  return (
    <Head>
        <script>(function(w,d,s,l,i){w[l]=w[l]||[];w[l].push({'gtm.start':
        new Date().getTime(),event:'gtm.js'});var f=d.getElementsByTagName(s)[0],
        j=d.createElement(s),dl=l!='dataLayer'?'&l='+l:'';j.async=true;j.src=
        'https://www.googletagmanager.com/gtm.js?id='+i+dl;f.parentNode.insertBefore(j,f);
        })(window,document,'script','dataLayer','GTM-W8CSNDP');</script>
      <title>{`NextJS Sanity eCommerce${title ? ` | ${title}` : ``}`}</title>
      <meta name="description" content={description} />
      <meta property="og:title" content={title} />
      <meta property="og:description" content={description} />

      {featuredImage && (
        <meta property="og:image" content={`${featuredImage}`} />
      )}
      {type && <meta property="og:type" content={type} />}
      <meta property="og:url" content={`${siteUrl}${Router.asPath}`} />
    </Head>
  );
};

export default MetaHead;
