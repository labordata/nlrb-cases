from scrapelib import Scraper
import typing


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

        response = self.get(self.base_url, params=params)

        print(response.request.url)


if __name__ == '__main__':
    import datetime

    s = NLRB()
    s.search()

    s.search(case_types=['R'])

    s.search(case_types=['R'], date_start=datetime.date(2021, 1, 1))

    s.search(case_types=['R'], date_start=datetime.date(2021, 1, 1), statuses=['Closed'])
