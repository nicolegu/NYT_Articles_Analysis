# Example of article data request

from NYT_API import NYTArticleExtractor
from config import NYT_API_KEY

def main():
    client = NYTArticleExtractor(api_key = NYT_API_KEY,
                                 fields = ['_id', 'headline', 'pub_date', 'abstract', 'keywords',
                                           'section_name', 'source', 'web_url'])
    articles_immigration = client.search_articles(query = 'immigration',
                                                  begin_date = '18510101',
                                                  end_date = '20250501',
                                                  results = 1000)
    
    articles_cleaned = client.process_multiple_articles(articles_immigration, strict_mode = False)

    client.save_to_csv(articles_cleaned, 'historical_immigration_articles.csv')

if __name__ == '__main__':
    main()




