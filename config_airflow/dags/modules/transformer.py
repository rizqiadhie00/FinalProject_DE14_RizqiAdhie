import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.sql import text, func
from sqlalchemy.orm import sessionmaker,relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Integer, Float, TIMESTAMP
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.inspection import inspect
import pandas as pd
import logging

Base = declarative_base()

class Case_Dim(Base):
    __tablename__ = 'dim_case'
    id = Column(Integer, primary_key=True)
    status_name = Column(String)
    status_detail = Column(String)
    status = Column(String)

class Province_Dim(Base):
    __tablename__ = 'dim_province'
    province_id = Column(String, primary_key=True)
    province_name = Column(String)

class District_Dim(Base):
    __tablename__ = 'dim_district'
    district_id = Column(String, primary_key=True)
    province_id = Column(String)
    district_name = Column(String)

class District_Daily(Base):
    __tablename__ = 'district_daily'
    id = Column(Integer, primary_key=True, autoincrement=True)
    district_id = Column(String)
    case_id = Column(Integer)
    date = Column(String)
    total = Column(Integer)

class Province_Daily(Base):
    __tablename__ = 'province_daily'
    id = Column(Integer, primary_key=True, autoincrement=True)
    province_id = Column(String)
    case_id = Column(Integer)
    date = Column(String)
    total = Column(Integer)

list_case = ['closecontact_dikarantina','closecontact_discarded','closecontact_meninggal','confirmation_meninggal','confirmation_sembuh','probable_diisolasi','probable_discarded','probable_meninggal','suspect_diisolasi','suspect_discarded', 'suspect_meninggal']
list_date = ['2020-08-05','2020-08-06','2020-08-07','2020-08-08','2020-08-09','2020-08-10']
list_cas_id = [1,2,3,4,5,6,7,8,9,10,11]

class Transformer():
    def __init__(self, db_mysql, db_postgre):
        self.db_mysql = db_mysql
        self.db_postgre = db_postgre

    
    def transform_dim_case(self):
        
        #Buat table untuk DIM Case
        Session = sessionmaker(bind=self.db_postgre)
        session = Session()

        Base.metadata.create_all(self.db_postgre)

        #Insert batch data, take data frame of panda as an example
        df = pd.DataFrame({"id":[1,2,3,4,5,6,7,8,9,10,11],
                            "status_name":['close_contact','close_contact','close_contact', 'confirmation', 'confirmation', 'probable', 'probable', 'probable', 'suspect', 'suspect', 'suspect'],
                            "status_detail":['dikarantina','discarded','meninggal','meninggal', 'sembuh', 'diisolasi','discarded','meninggal','diisolasi','discarded','meninggal'],
                            'status':['closecontact_dikarantina','closecontact_discarded','closecontact_meninggal','confirmation_meninggal','confirmation_sembuh','probable_diisolasi','probable_discarded','probable_meninggal','suspect_diisolasi','suspect_discarded', 'suspect_meninggal']})
        #The first insert method (pandas to SQL)
        #Pay attention to the if_exists parameter when using to_sql. If it is replace, it will drop the table first, then create the table, and finally insert the data
        df.to_sql('dim_case',con=self.db_postgre,if_exists='replace',index=False)


    def transform_dim_province(self):
        #Buat table untuk DIM Case
        Session = sessionmaker(bind=self.db_postgre)
        session = Session()

        Base.metadata.create_all(self.db_postgre)

        #Insert batch data, take data frame of panda as an example
        df = pd.DataFrame({"province_id":['1'],
                            "province_name":['jawa_barat']})
        #The first insert method (pandas to SQL)
        #Pay attention to the if_exists parameter when using to_sql. If it is replace, it will drop the table first, then create the table, and finally insert the data
        df.to_sql('dim_province',con=self.db_postgre,if_exists='replace',index=False)

    def transform_dim_district(self):
        #Buat table untuk DIM Case
        Session = sessionmaker(bind=self.db_postgre)
        session = Session()

        Base.metadata.create_all(self.db_postgre)

        #Insert batch data, take data frame of panda as an example
        df = pd.DataFrame({"district_id":['3204','3217','3216','3201','3207','3203','3209','3205','3212','3215','3208','3210','3218','3214','3213','3202','3211','3206','3273','3279','3275','3271','3277','3274','3276','3272','3278'],
                            "province_id":['32','32','32','32','32','32','32','32','32','32','32','32','32','32','32','32','32','32','32','32','32','32','32','32','32','32','32'],
                            'district_name':['Kabupaten Bandung', 'Kabupaten Bandung Barat', 'Kabupaten Bekasi', 'Kabupaten Bogor', 'Kabupaten Ciamis', 'Kabupaten Cianjur', 'Kabupaten Cirebon','Kabupaten Garut', 'Kabupaten Indramayu', 'Kabupaten Karawang', 'Kabupaten Kuningan', 'Kabupaten Majalengka','Kabupaten Pangandaran', 'Kabupaten Purwakarta', 'Kabupaten Subang','Kabupaten Sukabumi', ' Kabupaten Sumedang', 'Kabupaten Tasikmalaya', 'Kota Bandung', 'Kota Banjar', 'Kota Bekasi', 'Kota Bogor', 'Kota Cimahi', 'Kota Cirebon', 'Kota Depok', 'Kota Sukabumi', 'Kota Tasikmalaya']})
        #The first insert method (pandas to SQL)
        #Pay attention to the if_exists parameter when using to_sql. If it is replace, it will drop the table first, then create the table, and finally insert the data
        df.to_sql('dim_district',con=self.db_postgre,if_exists='replace',index=False)


    def transform_daily_district(self):
        conn_mysql = self.db_mysql.connect()
        
        df_mysql= conn_mysql.execute("SELECT * FROM covid_jabar")
        
        Session = sessionmaker(bind=self.db_postgre)
        session = Session()

        Base.metadata.create_all(self.db_postgre)

        for row in df_mysql.fetchall():
            for case in list_case:
                insert_stmt = insert(District_Daily.__table__).values(
                district_id=row['kode_kab'],case_id=list_cas_id[list_case.index(case)], date=row['tanggal'], total=row[case])

                insert_stmt = insert_stmt.on_conflict_do_update(
                    index_elements=[District_Daily.id],
                    set_={'district_id':row['kode_kab'],'case_id':list_cas_id[list_case.index(case)], 'date':row['tanggal'], 'total':row[case]})

                session.execute(insert_stmt)

                # new_case = District_Daily(district_id=row['kode_kab'],case_id=list_cas_id[list_case.index(case)], date=row['tanggal'], total=row[case])
                # #Only add, but not submit. If there is an error, you can also recall (rollback)
                # session.add(new_case)
                # #Commit to database
                # session.commit()


    def transform_daily_province(self):
        conn_mysql = self.db_mysql.connect()
        
        df_mysql= conn_mysql.execute("SELECT * FROM covid_jabar")
        
        #Buat table untuk DIM Case
        Session = sessionmaker(bind=self.db_postgre)
        session = Session()

        Base.metadata.create_all(self.db_postgre)

        # for row in df_mysql.fetchall():
        for case in list_case:
            for date in list_date:
                            
                results = conn_mysql.execute("SELECT SUM(covid_jabar."+case+") as a FROM covid_jabar WHERE covid_jabar.tanggal LIKE '" + date +"'")
                for row2 in results.fetchall():
                    total_sql = row2['a']

                    insert_stmt = insert(Province_Daily.__table__).values(
                    province_id=32,case_id=list_cas_id[list_case.index(case)], date=date, 
                    total=total_sql)

                    insp = inspect(self.db_postgre)
                    pk_constraint_name = insp.get_pk_constraint(Province_Daily.__tablename__)["name"]
                    # print(pk_constraint_name)  # tbl_foo_pkey

                    insert_stmt = insert_stmt.on_conflict_do_update(
                    constraint='province_daily_pkey',
                    set_={'province_id':32,'case_id':list_cas_id[list_case.index(case)], 'date':date, 
                    'total':total_sql})

                    session.execute(insert_stmt)

                    # new_case = Province_Daily(province_id=32,case_id=list_cas_id[list_case.index(case)], date=date, 
                    # total=total_sql)
                    #         #Only add, but not submit. If there is an error, you can also recall (rollback)
                    # session.add(new_case)
                    #         #Commit to database
                    # session.commit()

