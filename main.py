from __future__ import print_function

import os
import time
from bluelens_log import Logging

import bottlenose
from bs4 import BeautifulSoup

import redis
import pickle

from bl_db_product_amz_best.products import Products

# from stylelens_product.products import Products
# product_api = Products()
#
# HEALTH_CHECK_TIME = 60*20
REDIS_SERVER = os.environ['REDIS_SERVER']
REDIS_PASSWORD = os.environ['REDIS_PASSWORD']

REDIS_AMZ_BEST_ASIN_QUEUE = "bl:amz:best:asin:queue"
REDIS_AMZ_BEST_ASIN_QUEUE_TEST = "bl:amz:best:asin:queue:test"

options = {
  'REDIS_SERVER': REDIS_SERVER,
  'REDIS_PASSWORD': REDIS_PASSWORD
}
log = Logging(options, tag='bl-amazon-best-crawler')
rconn = redis.StrictRedis(REDIS_SERVER, port=6379, password=REDIS_PASSWORD)

amazon = bottlenose.Amazon(AWSAccessKeyId=os.environ['AWS_ACCESS_KEY_ID'],
                           AWSSecretAccessKey=os.environ['AWS_SECRET_ACCESS_KEY'],
                           AssociateTag=os.environ['AWS_ASSOCIATE_TAG'],
                           Parser=lambda text: BeautifulSoup(text, 'xml'))

def add_product(product):
  api_instance = Products()
  res = api_instance.add_product(product=product)
  print(res)

def call_item_lookup_api(node_id, asin):
  try:
    parsed_product = {}
    parsed_product['NodeId'] = node_id
    parsed_product['ASIN'] = asin

    item_lookup_res = amazon.ItemLookup(ItemId=asin,
                                        ResponseGroup='Accessories,'
                                                      'EditorialReview,'
                                                      'Images,'
                                                      'ItemAttributes,'
                                                      'OfferFull,'
                                                      'Offers,'
                                                      'PromotionSummary,'
                                                      'OfferSummary,'
                                                      'Reviews,'
                                                      'SalesRank,'
                                                      'Variations,'
                                                      'VariationSummary')

    items = item_lookup_res.find('Items')
    item_list = items.find_all('Item')
    for item in item_list:

      parsed_product['DetailPageURL'] = item.DetailPageURL.text
      if item.Binding:
        parsed_product['Binding'] = item.Binding.text
      if item.Department:
        parsed_product['Department'] = item.Department.text

      feature_list = []
      features = item.find_all('Feature')
      for feature in features:
        feature_list.append(feature.text)
      parsed_product['Feature'] = feature_list

      # if item.Label:
      #   parsed_product['Label'] = item.Label.text
      # if item.Manufacturer:
      #   parsed_product['Manufacturer'] = item.Manufacturer.text

      if item.Model:
        parsed_product['Model'] = item.Model.text

      parsed_product['ProductGroup'] = item.ProductGroup.text
      parsed_product['ProductTypeName'] = item.ProductTypeName.text

      # if item.Publisher:
      #   parsed_product['Publisher'] = item.Publisher.text

      # if item.Studio:
      #   parsed_product['Studio'] = item.Studio.text

      parsed_product['Title'] = item.Title.text

      item_attr = item.find('ItemAttributes')

      if item_attr:
        if item_attr.Title:
          parsed_product['Title'] = item_attr.Title.text

        if item_attr.Brand:
          parsed_product['Brand'] = item_attr.Brand.text

        if item_attr.Size:
          parsed_product['Size'] = item_attr.Size.text

        if item_attr.Color:
          parsed_product['Color'] = item_attr.Color.text

        # if item_attr.Label:
        #   parsed_product['Label'] = item_attr.Label.text

        # if item_attr.Manufacturer:
        #   parsed_product['Manufacturer'] = item_attr.Manufacturer.text

        # if item_attr.Model:
        #   parsed_product['Model'] = item_attr.Model.text

        if item_attr.MPN:
          parsed_product['MPN'] = item_attr.MPN.text

        if item_attr.ProductGroup:
          parsed_product['ProductGroup'] = item_attr.ProductGroup.text

        if item_attr.ProductTypeName:
          parsed_product['ProductTypeName'] = item_attr.ProductTypeName.text

        # if item_attr.Studio:
        #   parsed_product['Studio'] = item_attr.Studio.text

        # parsed_product['ItemDimensions'] = {}
        # if item_attr.ItemDimensions:
        #   for attr in item_attr.ItemDimensions:
        #     parsed_product['ItemDimensions'][attr.name] = attr.text

    # browse_nodes = soup.find('BrowseNodes')
    # browse_nodes_list = []
    # for browse_node in browse_nodes:
    #   browse_node_list = []
    #   if browse_node.Name:
    #     browse_node_list.append(browse_node.Name.text)
    #
    #   for children in browse_node.find_all('Children'):
    #     children_list = []
    #     for name in children.find_all('Name'):
    #       children_list.append(name.text)
    #     browse_node_list.append(children_list)
    #
    #   for ancestors in browse_node.find('Ancestors'):
    #     for name in ancestors.find_all('Name'):
    #       browse_node_list.append(name.text)
    #
    #   browse_nodes_list.append(browse_node_list)
    # parsed_product['BrowseNodes'] = browse_nodes_list


    # parsed_product['OfferSummary'] = {}
    offers_summary = item_lookup_res.find('OfferSummary')
    if offers_summary.LowestNewPrice:
      if offers_summary.LowestNewPrice.FormattedPrice:
        parsed_product['LowestNewPrice'] = offers_summary.LowestNewPrice.FormattedPrice.text

    # if offers_summary.TotalNew:
    #   parsed_product['OfferSummary']['TotalNew'] = offers_summary.TotalNew.text
    #
    # if offers_summary.TotalUsed:
    #   parsed_product['OfferSummary']['TotalUsed'] = offers_summary.TotalUsed.text
    #
    # if offers_summary.TotalCollectible:
    #   parsed_product['OfferSummary']['TotalCollectible'] = offers_summary.TotalCollectible.text
    #
    # if offers_summary.TotalRefurbished:
    #   parsed_product['OfferSummary']['TotalRefurbished'] = offers_summary.TotalRefurbished.text



    offers = item_lookup_res.find_all('Offer')
    # offers_list = []
    for offer in offers:
      # print(offer)
      # time.sleep(5)

      # offer_dic = {
      #   'Condition': []
      # }

      if offer.Merchant:
        if offer.Merchant.Name:
          parsed_product['Merchant'] = offer.Merchant.Name.text

      if offer.Price:
        parsed_product['Price'] = offer.Price.FormattedPrice.text

      if offer.SalePrice:
        parsed_product['SalePrice'] = offer.SalePrice.FormattedPrice.text

      # for condition in offer.find_all('Condition'):
      #   offer_dic['Condition'].append(condition.text)

      # offers_list.append(offer_dic)

    # parsed_product['Offer'] = offers_list
    # client.top_selling.insert_one(parsed_product)

    customer_reviews = item_lookup_res.find('CustomerReviews')
    for review in customer_reviews:
      if review.IFrameURL:
        parsed_product['CustomerReviewsURL'] = review.IFrameURL.text


    sales_rank = item_lookup_res.find('SalesRank')
    parsed_product['SalesRank'] = sales_rank.text

    add_product(parsed_product)

  except Exception as e:
    log.error(str(e))

def get_products(data):
  # log.info('analyze_product with FastText')
  products = pickle.loads(data)

  for product in products:
    node_id = product.get('node_id')
    asin = product.get('asin')

    # print('node_id: ' + node_id + ' / asin : ' + asin)
    call_item_lookup_api(node_id, asin)

def crawl_amazon_bests():
  try:
    while True:
      # key, value = rconn.blpop([REDIS_AMZ_BEST_ASIN_QUEUE_TEST])
      key, value = rconn.blpop([REDIS_AMZ_BEST_ASIN_QUEUE])
      if value is not None:
        get_products(value)

  except Exception as e:
    log.error(str(e))

def start(rconn):
  crawl_amazon_bests()

if __name__ == '__main__':
  try:
    # log.info('Start bl-amazon-best-crawler')
    start(rconn)
  except Exception as e:
    log.error(str(e))
