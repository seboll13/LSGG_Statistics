import pandas as pd

YEAR = 2022

class Reader:
    def __init__(self, year):
        self.year = year
        self.useless_columns = ['ICAO24','Classe','Poids','Desc.','Propriétaire', 'Groupe', 'Redevance Bruit']

    def read(self):
        df = pd.read_csv(f'data/flights_{self.year}.csv', header=0, names=['ICAO24','Reg.','A_Type','Classe','Poids','Desc.','Indicatif','Heure','Date','Jour','M_Type','Piste',"Compagnie d'Aviation",'Propriétaire','Code du Pays','Groupe','Vol','Vers / De','Redevance Bruit'])
        return df.drop(columns=self.useless_columns) 


if __name__ == '__main__':
    reader = Reader(YEAR)
    df = reader.read()
    filter_cond = (df['Compagnie d\'Aviation'] == 'easyJet') | (df['Compagnie d\'Aviation'] == 'easyJet Switzerland')
    print(df.loc[filter_cond])
    
    # beauty print for few values
    #print(tabulate(df.loc[filter_cond], headers = 'keys', tablefmt = 'psql'))