import sqlite3
from stuf import stuf

import unicodedata
import regex

def normalize_text(text):
  text = unicodedata.normalize('NFKD', text.strip().lower())
  text = regex.subn(r'\p{P}+', '', text)[0]
  return text.encode('ascii', 'ignore').decode()

all_rules = ['501', 'cricket']
all_bets = [25, 50, 100, 250, 500, 1000, 2500, 5000, 10000]

def gen_username():
  for w in range(2,5):
    for i in range(100):
      u = random_name(w)
      n = normalize_text(u)
      yield u, n

class UniquenessError(ValueError):
  pass

def signup():
  for u, n in gen_username():
    try:
      user = User(username=u, norm_username=n)
      user.save()
      return user
    except UniquenessError:
      pass

class Country(Storable):
  schema = dict(
    alpha2=Text(max=2, min=2, before_save=lambda x: x.strip().upper()))

class User(Storable):
  schema = dict(
    id=Integer(autoincrement=True),
    username=Text(not_null=True, default=gen_username),
    norm_username=Text(not_null=True, unique=True),
    country=FK(cls=Country, field='alpha2'),
    password=Text(),
    photo=Text(encoding='base64'),
    photo_flags=Integer(),
    coins=Integer(min=0, default=50))

  def before_save(self):
    self.norm_username = normalize_text(self.username)

  def credit(self, coins):
    pass

class Match(Storable):
  schema = dict(
    id=Integer(autoincrement=True),
    rules=Enum(values=all_rules),
    bet=Enum(values=all_bets),
    started_at=DateTime(),
    _pk='id')

class MatchUser:
  match = FK(cls=Match, field='id')
  user = FK(cls=User, field='id')
  client_state = Text(encoding='json')
  _pk = ['match', 'user']


