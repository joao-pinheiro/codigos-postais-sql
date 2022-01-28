import csv
from typing import List


def filter(row) -> List:
    result = []
    for r in row:
        result.append(r.replace("'", "''"))
    return result

class SqlDistricts:

    def create_table(self) -> str:
        return """
        CREATE TABLE district(
        id_district CHAR(2) NOT NULL UNIQUE PRIMARY KEY,
        name VARCHAR(255) NOT NULL
        );
        """

    def sql_insert(self, row) -> str:
        return "INSERT INTO district(id_district, name) VALUES('{}', '{}');".format(row[0], row[1])


class SqlCouncils:

    def create_table(self) -> str:
        return """
        CREATE TABLE council(
        id_council CHAR(4) NOT NULL UNIQUE PRIMARY KEY,
        fk_district CHAR(2) NOT NULL REFERENCES district,
        name VARCHAR(255) NOT NULL
        );
        """

    def sql_insert(self, row) -> str:
        return "INSERT INTO council(id_council, fk_district, name) VALUES('{}', '{}', '{}');".format(row[0] + row[1],
                                                                                                     row[0], row[2])


class SqlTown:

    def create_table(self) -> str:
        return """
        CREATE TABLE town(
        id_town VARCHAR(16) NOT NULL UNIQUE PRIMARY KEY,
        fk_district CHAR(2) NOT NULL REFERENCES district,
        fk_council CHAR(4) NOT NULL REFERENCES council,        
        name TEXT
        );
        """

    def sql_insert(self, row) -> str:
        council = row[0] + row[1]
        return "INSERT INTO town(id_town, fk_district, fk_council, name) VALUES('{}','{}','{}', '{}');".format(row[2],
                                                                                                               row[0],
                                                                                                               council,
                                                                                                               row[3])


class SqlCp:

    def create_table(self) -> str:
        return """
        CREATE TABLE cp(
        fk_district CHAR(2) NOT NULL REFERENCES district,
        fk_council CHAR(4) NOT NULL REFERENCES council,
        fk_town VARCHAR(16) NOT NULL REFERENCES town,
        street TEXT NOT NULL,
        cp VARCHAR(5) NOT NULL,
        cp_street VARCHAR(5) NOT NULL,
        cp_locality TEXT NOT NULL
        );
        """

    def sql_insert(self, row) -> str:
        chunks = []
        for i in range(5, 13):
            if len(row[i]) > 0:
                chunks.append(row[i])

        return "INSERT INTO cp(fk_district, fk_council, fk_town, street, cp, cp_street, cp_locality) VALUES('{}', '{}', '{}', '{}', '{}', '{}', '{}');".format(
            row[0],  # district
            row[0] + row[1],  # council
            row[2],  # town id
            " ".join(chunks),  # street
            row[14],  # cp code
            row[15],  # cp street
            row[16]  # locality
        )


transformer = SqlDistricts()
with open('data/distritos.txt', encoding='iso8859-1') as src:
    with open('sql/distritos.sql', encoding='utf-8', mode="w+") as dest:
        dest.write(transformer.create_table() + "\n")
        reader = csv.reader(src, delimiter=';')
        for row in reader:
            if len(row) == 2:
                dest.write(transformer.sql_insert(filter(row)) + "\n")

transformer = SqlCouncils()
with open('data/concelhos.txt', encoding='iso8859-1') as src:
    with open('sql/concelhos.sql', encoding='utf-8', mode="w+") as dest:
        dest.write(transformer.create_table() + "\n")
        reader = csv.reader(src, delimiter=';')
        for row in reader:
            if len(row) == 3:
                dest.write(transformer.sql_insert(filter(row)) + "\n")

transformer = SqlTown()
town_list = []
with open('data/todos_cp.txt', encoding='iso8859-1') as src:
    with open('sql/localidades.sql', encoding='utf-8', mode="w+") as dest:
        dest.write(transformer.create_table() + "\n")
        reader = csv.reader(src, delimiter=';')
        for row in reader:
            if row[2] not in town_list:
                if len(row) == 17:
                    town_list.append(row[2])
                    dest.write(transformer.sql_insert(filter(row)) + "\n")

transformer = SqlCp()
with open('data/todos_cp.txt', encoding='iso8859-1') as src:
    with open('sql/cp.sql', encoding='utf-8', mode="w+") as dest:
        dest.write(transformer.create_table() + "\n")
        reader = csv.reader(src, delimiter=';')
        for row in reader:
            if len(row) == 17:
                dest.write(transformer.sql_insert(filter(row)) + "\n")
