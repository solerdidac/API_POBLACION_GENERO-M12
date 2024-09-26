-- Crear Base de Datos
CREATE DATABASE IF NOT EXISTS poblacio_genero;
USE poblacio_genero;    

-- Crear la tabla de districtes
CREATE TABLE districte (
    id_districte INT NOT NULL PRIMARY KEY,
    nom_districte VARCHAR(100) NOT NULL
);

-- Crear la tabla de barris
CREATE TABLE barri (
    id_barri INT NOT NULL PRIMARY KEY,
    nom_barri VARCHAR(100) NOT NULL,
    id_districte INT NOT NULL,
    aeb INT NOT NULL,
    FOREIGN KEY (id_districte) REFERENCES districte(id_districte) ON DELETE CASCADE
);

-- Crear la tabla de seccions censals
CREATE TABLE seccio_censal (
    id_seccio_censal INT AUTO_INCREMENT PRIMARY KEY,
    id_barri INT NOT NULL,
    codi_seccio_censal INT NOT NULL,
    FOREIGN KEY (id_barri) REFERENCES barri(id_barri) ON DELETE CASCADE
);

-- Crear la tabla de població
CREATE TABLE poblacio (
    id_poblacio INT AUTO_INCREMENT PRIMARY KEY,
    id_seccio_censal INT NOT NULL,
    data_referencia DATE NOT NULL,
    sexe INT NOT NULL, -- 1 hombres, 2 para mujeres
    valor INT NOT NULL,
    FOREIGN KEY (id_seccio_censal) REFERENCES seccio_censal(id_seccio_censal) ON DELETE CASCADE
);
