from flask import Flask
from routes import bands_blueprint

app = Flask(__name__)

# Register the blueprint
app.register_blueprint(bands_blueprint)


if __name__ == '__main__':
    app.run(debug=True, port=5001)
