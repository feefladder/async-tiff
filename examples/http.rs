use std::sync::Arc;

use async_tiff::{
    error::AsyncTiffResult,
    reader::{/*CacheReader, */PrefetchReader, ReqwestReader},
};
use reqwest::Url;

#[tokio::main]
async fn main() -> AsyncTiffResult<()> {
    let hrefs = [
        "https://sentinel-cogs.s3.us-west-2.amazonaws.com/sentinel-s2-l2a-cogs/16/T/CR/2025/3/S2A_16TCR_20250322_0_L2A/B02.tif",
        "https://isdasoil.s3.amazonaws.com/soil_data/bulk_density/bulk_density.tif",
        "https://isdasoil.s3.amazonaws.com/covariates/dem_30m/dem_30m.tif",
        /*non-cog*/ "https://zenodo.org/records/4087905/files/sol_db_od_m_30m_0..20cm_2001..2017_v0.13_wgs84.tif",
        /*non-cog*/ "https://zenodo.org/records/4091154/files/sol_log.wpg2_m_30m_0..20cm_2001..2017_v0.13_wgs84.tif",
        "https://service.pdok.nl/rws/ahn/atom/downloads/dtm_05m/M_01GN2.tif",
        "https://service.pdok.nl/rws/ahn/atom/downloads/dtm_05m/M_02DZ1.tif",
    ];
    // let href= if let Some(arg) = std::env::args().nth(1) {
    //     hrefs[arg.parse().unwrap_or(0) % hrefs.len()]
    // } else {
    //     hrefs[0]
    // };
    // println!("processing {href:?}");

    

    for href in hrefs {
        println!("processing {href:?}");
        let reader = Arc::new(
            PrefetchReader::new(
                /*Arc::new(CacheReader::new(*/Arc::new(ReqwestReader::new(
                    reqwest::Client::new(),
                    Url::parse(href.into()).unwrap(),
                )),//)),
                16 * 1024,
            )
            .await?,
        );
        let tiff = async_tiff::TIFF::try_open(reader).await?;
        println!("tiff with {:?} ifds", tiff.ifds().as_ref().len());
    }
    
    Ok(())
}
