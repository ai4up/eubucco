import pandas as pd
import os
root = '/p/projects/eubucco'

lazio_g = pd.read_csv(os.path.join(root,'data/1-intermediary-outputs-v0_1/italy/lazio-gov-3035_geoms_with_d.csv'))
lazio_a = pd.read_csv(os.path.join(root,'data/1-intermediary-outputs-v0_1/italy/lazio-gov_attrib_with_d.csv'))
print(len(lazio_g))
print(len(lazio_a))

df = pd.merge(lazio_g[['id','geometry']],lazio_a.drop(columns=['id']),left_index=True,right_index=True)
print(len(df))

dup = df[df.duplicated(subset=['id'],keep=False)]
dup = dup[~dup.duplicated(subset=['id','geometry'])]
dup['id'] = dup['id'] + '_' + dup.index.to_series().astype(str)

df = df[~df.duplicated(subset=['id'],keep=False)]
df = pd.concat([df,dup])

print('-----')
print(len(df))

df[['id','geometry']].to_csv(os.path.join(root,'data/1-intermediary-outputs-v0_1/italy/lazio-gov-3035_geoms.csv'),index=False)
df.drop(columns='geometry').to_csv(os.path.join(root,'data/1-intermediary-outputs-v0_1/italy/lazio-gov_attrib.csv'),index=False)


