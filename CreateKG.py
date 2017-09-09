# coding=utf-8
from py2neo import Graph, Node, Relationship , Schema
from py2neo.ogm import GraphObject,Property,Related,RelatedObjects,RelatedFrom,RelatedTo
import pandas as pd
def Create_node(graph,node):
    graph.create(node)
def Create_relation(node1,relation,node2):
    rel = Relationship(node1,relation,node2)
    graph.create(rel)

graph = Graph(
    "http://localhost:7474",
    username="neo4j",
    password="bl44jn"
)
class Company(GraphObject):
    __primarylabel__ = 'Company'
    __primarykey__ = 'code'

    name = Property()
    code = Property()
    enname = Property()
    ipodate = Property()
    comp_name = Property()
    business = Property()
    briefing = Property()
    employeenum = Property()
    office = Property()
    crtindpdirector = Property()
    frmindpdirector = Property()
    mkt = Property()

    holder = Related('Person','holder')  # 股东
    ceo = Related('Person','ceo')  # 总经理
    chairman = Related('Person','chairman')  # 法人
    boardchairmen = Related('Person','boardchairmen')  # 董事长
    manager = Related('Person','manager')  # 高管
    discloser = Related('Person','discloser') #披露人

    indus = Related('Industry','industry') #行业
    concept = Related('Concept','concept')
    city = Related('City','city')
    province = Related("Province",'province')
    nature = Related("Nature",'nature')
    product_type = Related("ProductType",'product_type')
    product_name = Related("ProductName",'product_name')

class Person(GraphObject):
    __primarylabel__ = 'Person'

    name = Property()
    degree = Property()
    position = Property()
    gender = Property()
    nationality = Property()

    holder = Related(Company,'holder')
    ceo = Related(Company, 'ceo')  # 总经理
    chairman = Related(Company, 'chairman')  # 法人
    boardchairmen = Related(Company, 'boardchairmen')  # 董事长
    manager = Related(Company, 'manager')  # 高管
    discloser = Related(Company, 'discloser')  # 披露人

class Industry(GraphObject):
    __primarylabel__ = 'Industry'
    __primarykey__ = 'name'

    name = Property()
    indus = Related(Company, 'industry')  # 行业

class Concept(GraphObject):
    __primarylabel__ = "Concept"
    __primarykey__ = "name"

    name = Property()
    concept = Related(Company, 'concept')
class Nature(GraphObject):
    __primarylabel__ = "Nature"
    __primarykey__ = "name"

    name = Property()
    nature = Related(Company, 'nature')

class Province(GraphObject):
    __primarylabel__ = 'Province'
    __primarykey__ = 'name'

    name = Property()
    city = Related('City','location')
    province = Related(Company, 'province')


class City(GraphObject):
    __primarylabel__ = 'City'
    __class__ = 'name'

    name = Property()
    city = Related(Company, 'city')
    province = Related(Province,'location')


class ProductType(GraphObject):
    __primarylabel__ = "ProductType"
    __primarykey__ = "name"

    name = Property()
    product_type = Related(Company, 'product_type')

class ProductName(GraphObject):
    __primarylabel__ = "ProductName"
    __primarykey__ = "name"

    name = Property()
    product_name = Related(Company, 'product_name')




data = pd.read_csv('info.csv',encoding='utf8')
for i in range(len(data)):
    code = data.loc[i].Code
    print code,' ',i,'/',len(data)
    name = data.loc[i].Name
    enname = data.loc[i].sec_englishname
    ipodate = data.loc[i].ipo_date
    mkt = data.loc[i].mkt
    concept = data.loc[i].concept
    comp_name = data.loc[i].comp_name
    nature = data.loc[i].nature1
    chairman = data.loc[i].chairman
    business = data.loc[i].business
    briefing = data.loc[i].briefing
    majorproducttype = data.loc[i].majorproducttype
    majorproductname = data.loc[i].majorproductname
    employeenum = data.loc[i].employee
    province = data.loc[i].province
    city = data.loc[i].city
    office = data.loc[i].office
    discloser = data.loc[i].discloser
    industry = data.loc[i].industry_citic
    boardchairmen = data.loc[i].boardchairmen
    ceo = data.loc[i].ceo
    crtindpdirector = data.loc[i].crtindpdirector
    frmindpdirector = data.loc[i].frmindpdirector
    holder_name1 = data.loc[i].holder_name1
    holder_name2 = data.loc[i].holder_name2

    # Company_node
    company_node = Company()
    company_node.name = name
    company_node.code = code
    company_node.enname = enname
    company_node.ipodate = ipodate
    company_node.comp_name = comp_name
    company_node.business = business
    company_node.briefing = briefing
    company_node.employeenum = employeenum
    company_node.office = office
    company_node.mkt = mkt
    company_node.crtindpdirector = crtindpdirector
    company_node.frmindpdirector = frmindpdirector
    graph.push(company_node)

    #Person_node
    #chairman
    chairman_node = Person()
    chairman_node.name = chairman
    chairman_node.position = 'chairman'
    chairman_node.chairman.add(company_node)
    graph.push(chairman_node)
    #ceo
    ceo_node = Person()
    ceo_node.name = ceo
    ceo_node.position = 'ceo'
    ceo_node.ceo.add(company_node)
    graph.push(ceo_node)
    #dicloser
    discloser_node = Person()
    discloser_node.name = discloser
    discloser_node.position = 'discloser'
    discloser_node.discloser.add(company_node)
    graph.push(discloser_node)
    #boardchairmen
    boardchairmen_node = Person()
    boardchairmen_node.name = boardchairmen
    boardchairmen_node.position = 'boardchairman'
    boardchairmen_node.boardchairmen.add(company_node)
    graph.push(boardchairmen_node)
    #holder_1
    holder_1 = Person()
    holder_1.name = holder_name1
    holder_1.position = 'holder'
    holder_1.holder.add(company_node)
    graph.push(holder_1)
    #holder_2
    holder_2 = Person()
    holder_2.name = holder_name2
    holder_2.position = 'holder'
    holder_2.holder.add(company_node)
    graph.push(holder_2)
    #Industry_node
    indus_node = Industry()
    indus_node.name = industry
    indus_node.indus.add(company_node)
    graph.push(indus_node)
    #Nature_node
    nature_node = Nature()
    nature_node.name = nature
    nature_node.nature.add(company_node)
    graph.push(nature_node)
    #Province_node
    province_node = Province()
    province_node.name = province
    province_node.province.add(company_node)
    graph.push(province_node)
    #City_node
    city_node = City()
    city_node.name = city
    city_node.province.add(province_node)
    city_node.city.add(company_node)
    graph.push(city_node)
    #Concept_node
    concept_node = Concept()
    concept_node.name = concept
    concept_node.concept.add(company_node)
    graph.push(concept_node)
    #Product_name
    for tmp in majorproductname.split(';'):
        productname_node = ProductName()
        productname_node.name = tmp
        productname_node.product_name.add(company_node)
        graph.push(productname_node)
    #Product_type
    for tmp in majorproducttype.split(';'):
        producttype_node = ProductType()
        producttype_node.name = tmp
        producttype_node.product_type.add(company_node)
        graph.push(producttype_node)



