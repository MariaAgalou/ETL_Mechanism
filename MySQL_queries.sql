--- SQL queries to create the tables in MySQL database ---

CREATE TABLE Bands (
    band_id VARCHAR(100),
    band_name VARCHAR(50),
    date_of_formation VARCHAR(50),
    band_members INT,
    PRIMARY KEY (band_id)
);

CREATE TABLE Users (
    user_id INT,
    username VARCHAR(100),
    PRIMARY KEY (user_id)
);

CREATE TABLE Albums (
    album_id INT AUTO_INCREMENT,
    album_title VARCHAR(100),
    band_id VARCHAR(100),
    UNIQUE (album_title),
    PRIMARY KEY (album_id),
    FOREIGN KEY (band_id) REFERENCES Bands(band_id)
);

CREATE TABLE User_favorites (
    user_f_id INT AUTO_INCREMENT,
    user_id INT,
    band_id VARCHAR(100),
    PRIMARY KEY(user_f_id),
    FOREIGN KEY (user_id) REFERENCES Users(user_id),
    FOREIGN KEY (band_id) REFERENCES Bands(band_id)
);

--- SQL query to load data saved in MySQL tables ---

SELECT
    u.username,
    b.band_name,
    a.album_title
FROM
    users u
JOIN
    user_favorites uf ON u.user_id = uf.user_id
JOIN
    bands b ON uf.band_id = b.band_id
JOIN
    albums a ON b.band_id = a.band_id;