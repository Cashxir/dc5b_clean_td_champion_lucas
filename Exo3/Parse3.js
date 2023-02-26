const fs = require('fs');
const csv = require('csv-parser');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;

const inputFilePath = 'electronic-card-transactions-december-2022-csv-tables.csv';
const outputFilePath = 'result.csv';

// Créer un objet CSV Writer pour écrire les quatre colonnes, y compris l'ID auto-incrémenté
const csvWriter = createCsvWriter({
  path: outputFilePath,
  header: [
    { id: 'id', title: 'id' },
    { id: 'Period', title: 'Period' },
    { id: 'Data_value', title: 'Data_value' },
    { id: 'Series_title_2', title: 'Series_title_2' },
  ]
});

// Créer un tableau pour stocker les lignes non vides et qui ont la bonne valeur dans la colonne "Series_title_2"
const rows = [];
let id = 1; // Initialiser l'ID à 1

// Lire le fichier CSV ligne par ligne
fs.createReadStream(inputFilePath)
  .pipe(csv())
  .on('data', (row) => {
    // Vérifier si la ligne contient des cellules vides dans les colonnes requises
    const isRowEmpty = [row.Period, row.Data_value, row.Series_title_2].some((cell) => cell === '');

    // Vérifier si la ligne contient la bonne valeur dans la colonne "Series_title_2" et n'a pas de cellules vides
    if (!isRowEmpty && ["Credit", "Debit", "Services"].includes(row.Series_title_2)) {
      const filteredRow = {
        id: id++, // Incrémenter l'ID pour chaque ligne non vide
        Period: row.Period,
        Data_value: row.Data_value,
        Series_title_2: row.Series_title_2,
      };
      rows.push(filteredRow);
    }
  })
  .on('end', () => {
    // Écrire les lignes non vides et qui ont la bonne valeur dans la colonne "Series_title_2" dans un nouveau fichier CSV
    csvWriter
      .writeRecords(rows)
      .then(() => console.log('Nettoyage terminé'));
  });
