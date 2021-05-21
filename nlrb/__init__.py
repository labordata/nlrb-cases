from scrapelib import Scraper
import lxml.html
import typing
import requests
from lxml import etree


CaseTypes = typing.Sequence[typing.Literal['C', 'R']]
Statuses = typing.Sequence[typing.Literal['Open', 'Closed', 'Open - Blocked']]


class NLRB(Scraper):
    base_url = 'https://www.nlrb.gov/search/case'

    def search(self,
               case_types: typing.Optional[CaseTypes] = None,
               statuses: typing.Optional[Statuses] = None,
               date_start: typing.Optional['datetime.date'] = None,
               date_end: typing.Optional['datetime.date'] = None):

        params = {}
        if case_types:
            params['f[0]'] = '({})'.format(
                ' OR '.join('case_type:' + case_type
                            for case_type in case_types))
        if statuses:
            for i, status in enumerate(statuses):
                params['s[{}]'.format(i)] = status

        if date_start:
            params['date_start'] = date_start.strftime('%m/%d/%Y')
            if not date_end:
                params['date_end'] = datetime.date.today().strftime('%m/%d/%Y')

        if date_end:
            params['date_end'] = date_end.strftime('%m/%d/%Y')
            if not date_start:
                params['date_start'] = '1/1/1960'

        search_results = self.get(self.base_url, params=params)
        yield from self._parse_search_results(search_results)

        for results in self._paginate(search_results):
            yield from self._parse_search_results(results)

    def _parse_search_results(self, response: 'requests.models.Response'):
        page = lxml.html.fromstring(response.text)
        page.make_links_absolute(response.request.url)
        for result in page.xpath("//div[@class='wrapper-div']"):
            result_dict = {}

            name, = result.xpath("./div[@class='type-div']//a/text()")
            result_dict['name'] = name.strip()

            left_column = result.xpath(".//div[@class='left-div']/strong")
            right_column = result.xpath(".//div[@class='right-div']/strong")
            columns = left_column + right_column

            for header_element in columns:
                header = header_element.text.strip(': ').lower()
                if header == 'case number':
                    link = header_element.getnext()
                    result_dict[header] = link.text.strip()
                    result_dict['url'] = link.get('href')
                elif header == 'date filed':
                    date_str = header_element.tail
                    result_dict[header] = datetime.datetime.strptime(date_str, '%B %d, %Y').date()
                else:
                    result_dict[header] = header_element.tail

            yield result_dict

    def _paginate(self, response: 'requests.models.Response'):
        ...

    def case_details(self, case_number: str):
        ...

    def _parse_case_details(self, response: 'requests.models.Response'):
        ...


if __name__ == '__main__':
    import datetime

    s = NLRB()
    print(next(s.search()))

    s.search(case_types=['R'])

    s.search(case_types=['R'], date_start=datetime.date(2021, 1, 1))

    s.search(case_types=['R'], date_start=datetime.date(2021, 1, 1), statuses=['Closed'])
