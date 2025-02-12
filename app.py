from flask import Flask, render_template, request


# python3 -m venv mon_environnement
# source mon_environnement/bin/activate
# deactivate
# pip freeze > requirements.txt
# pip install -r requirements.txt
app = Flask(__name__)


if __name__ == "__main__":
    app.run(debug=True)
