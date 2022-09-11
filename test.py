from requests_html import HTMLSession

url = "https://www.glassdoor.com.mx/Prestaciones/Walmart-México-Prestaciones-EI_IE715.0,7_IL.8,14_IN169.htm"

session = HTMLSession()
r = session.get(url)

r.html.render(sleep=2)

print(r.html.xpath('//h1/text()', first=True))
