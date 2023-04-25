from aggregators.brand.brand_au_aggregator import BrandAuAggregator

from aggregators.brand.brand_jp_aggregator  import BrandJpAggregator
from aggregators.brand.brand_us_aggregator  import BrandUsAggregator
from aggregators.brand.brand_ca_aggregator  import BrandCaAggregator
from aggregators.brand.brand_fr_aggregator  import BrandFrAggregator


classmap = {
 
  "brand_au_aggregator": BrandAuAggregator,
  "brand_jp_aggregator" : BrandJpAggregator,
  "brand_us_aggregator" : BrandUsAggregator,
  "brand_ca_aggregator" : BrandCaAggregator,
  "brand_fr_aggregator" : BrandFrAggregator,


}