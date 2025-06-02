import csv
from pynytimes import NYTAPI
import pandas as pd
from datetime import datetime
import logging
import json
import requests
import time

# Set up logging
logging.basicConfig(
    level = logging.INFO,
    format = '%(asctime)s - %(levelname)s - %(message)s',
    filename = 'nyt_extraction.log'
)

# Customize exception
class FieldMissingError(Exception):
    pass

class NYTArticleExtractor():

    def __init__(self, api_key, base_url, fields = None):
        """
        Initialize the NYT article extractor

        Args:
            api_key (str): NYT API key
            base_url (str): NYT API url, default is the Article Search API url
            fields (list): fields that must be present for each article
        """
        # self.nyt = NYTAPI(api_key, parse_dates = True)
        self.api_key = api_key
        self.base_url = base_url or 'https://api.nytimes.com/svc/search/v2/articlesearch.json'
        self.fields = ['_id', 'headline', 'pub_date'] or fields # Must have id, headline, and published date
        self.all_fields = [
            '_id', 'headline', 'byline', 'abstract', 'snippet', 'source', 'print_page', 'multimedia',
            'document_type', 'web_url',
            'pub_date', 'news_desk', 'section_name', 'subsection_name', 'type_of_material',
            'word_count', 'uri',
            'keywords', 'print_section'
        ]

    def search_articles(self, query, begin_date, end_date, results = 10, start_page = 0):
        """
        Search for articles based on query and date range
        
        Args:
            query (str): search query
            begin_date (str): beginning of date range, format YYYYMMDD
            end_date (str): end of date range, format YYYYMMDD
            results (int): number of results to return, default is 10
            start_page (int): index of start page, default is 0

        Returns:
            list: raw article data from NYT API
        """
        try:
            all_articles = []

            pages_needed = (results + 9) // 10 # only 10 results per page per request

            pages_needed = min(pages_needed, 100) # limit to 100 pages

            logging.info(f"Fetching up to {results} articles across {pages_needed} pages")

            for page in range(page, pages_needed):
                params = {
                    'api-key': self.api_key,
                    'q': query,
                    'page': page
                }

                if begin_date:
                    params['begin_date'] = begin_date

                if end_date:
                    params['end_date'] = end_date

                response = requests.get(self.base_url, params=params)
                response.raise_for_status()
                data = response.json()

                articles = data['response']['docs']

                if not articles:
                    logging.info(f"No more articles found after page {page}")
                    break

                all_articles.extend(articles)
                logging.info(f"Retrieved {len(articles)} articles from page {page}")

                if len(all_articles) >= results:
                    all_articles = all_articles[:results]
                    break

                if page < page + pages_needed - 1:
                    time.sleep(12)

            logging.info(f"Total articles retrieved: {len(all_articles)} for query: '{query}")
            return all_articles
        
        except Exception as e:
                print(f"Error searching articles: {e}")
                return []

    def extract_nested_field(self, article, field_path):
        """
        Extract a potentially nested field from an article

        Args:
            article (dict): article data
            field_path (str): path to field, using dot to separate nested fields

        Returns:
            field value or None if field not found
        """
        paths = field_path.split('.')
        value = article

        for path in paths:
            # check if value is a dictionary and get values in nested fields recursively
            if isinstance(value, dict) and path in value:
                value = value[path]
            else:
                return None
                
        return value
    
    def process_article(self, article):
        """
        Process a single article and extract all possible fields

        Args:
            article (dict): article data

        Returns:
            dict: processed article data

        Raises:
            FieldMissingError: if a required field is missing

        """
        processed = {}

        processed['headline'] = self.extract_nested_field(article, 'headline.main')
        processed['headline_kicker'] = self.extract_nested_field(article, 'headline.kicker')
        processed['headline_print'] = self.extract_nested_field(article, 'headline.print_headline')

        processed['byline'] = self.extract_nested_field(article, 'byline.original')

        processed['image_url'] = self.extract_nested_field(article, 'multimedia.default.url')
        
        if 'keywords' in article and article['keywords']:
            try:
                processed['keywords'] = ','.join([kw.get('value', '') for kw in article['keywords']])
            except (AttributeError, TypeError):
                processed['keywords'] = ''
        else:
            processed['keywords'] = ''

        for field in self.all_fields:
            if field not in ['headline', 'byline', 'multimedia', 'keywords']:
                if field in article:
                    if isinstance(article[field], (dict, list)):
                        processed[field] = json.dumps(article[field]) # Convert Python object to json string
                    else:
                        processed[field] = article[field]
                else:
                    processed[field] = None

        for field in self.required_fields:
            if field in processed and processed[field] is None:
                raise FieldMissingError(f"Required field '{field}' is missing from article with id: {article.get('_id', 'unknown')}")
            
        return processed
    
    def process_multiple_articles(self, articles, strict_mode = False):
        """
        Process multiple articles and handle errors

        Args:
            articles (list): list of raw articles
            strict_mode (bool): if True, raise exception on first missing field
                                if False, log error and continue

        Returns:
            list: processed articles
        """

        processed_articles = []
        errors = 0

        for i, article in enumerate(articles):
            try:
                processed = self.process_article(article)
                processed_articles.append(processed)
            except FieldMissingError as e:
                errors += 1
                if strict_mode:
                    raise
                else:
                    logging.error(f"Skipping article {i+1}: {str(e)}")
            except Exception as e:
                errors += 1
                logging.error(f"Error processing article {i+1}: {str(e)}")
                if strict_mode:
                    raise
        
        logging.info(f"Processed {len(processed_articles)} articles successfully with {errors} errors")
        return processed_articles
    
    def save_to_csv(self, articles, filename = 'nyt_articles.csv'):
        """
        Save processed articles to csv file

        Args:
            articles (list): processed articles
            filename (str): output filename
        """
        if not articles:
            logging.warning('No articles to save')
            return
        
        try:
            df = pd.DataFrame(articles)
            df.to_csv(filename, index = False, encoding = 'utf-8')
            logging.info(f"Successfully saved {len(articles)} articles to {filename}")
        except Exception as e:
            logging.error(f"Error saving to CSV: {str(e)}")
            raise