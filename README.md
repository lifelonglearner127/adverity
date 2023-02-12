# adverity
## Rooms for Improvements
- Orjson for better JSON serialization
- Create only one ClientSession when django starts
- From Django3.0, it natively supports async. Leveraging it might be helpful
- Add testing

## Installation
```shell
pyenv install 3.9
git clone
pyenv local 3.9
poetry install
poetry shell
python manage.py migrate
python manage.py runserver
```
