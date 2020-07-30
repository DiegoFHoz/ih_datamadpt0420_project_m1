
def percent_country (data_table, country='all'):
    group_data = data_table[['country_l', 'habitat', 'uuid','title']].groupby(['country_l','habitat','title']).count().reset_index()
    total = group_data['uuid'].sum()
    group_data['Percentage'] = group_data['uuid'].apply(lambda x: (x/total)*100).round(3).astype(str) + '%'
    if country == 'all':
        group_data = group_data.rename(columns={'country_l': 'Country', 'habitat': 'Habitat', 'uuid': 'Quantity'})
        group_data.to_csv('data/results/all_country_analysis.csv', index=False)
        print('exporting report from all countries...')

    else:
        group_data = group_data[group_data['country_l'] == country]
        group_data = group_data.rename(columns={'country_l': 'Country', 'habitat': 'Habitat', 'uuid': 'Quantity'})
        group_data.to_csv(f'data/results/{country}.csv', index=False)
        print(f'exporting report from {country}')

    return group_data