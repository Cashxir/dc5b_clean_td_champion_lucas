const fs = require('fs');
const csv = require('csv-parser');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;

const inputFilePath = 'electronic-card-transactions-december-2022-csv-tables.csv';
const outputFilePath = 'result.csv';

// Création d'un CSV Writer avec les 4 colonnes 
const csvWriter = createCsvWriter({
  path: outputFilePath,
  header: [
    { id: 'id', title: 'id' },
    { id: 'Period', title: 'Period' },
    { id: 'Data_value', title: 'Data_value' },
    { id: 'Series_title_2', title: 'Series_title_2' },
  ]
});

// Créer un tableau pour stocker les lignes"Series_title_2"
const rows = [];
let id = 1; 

// Lecture du fichier CSV
fs.createReadStream(inputFilePath)
  .pipe(csv())
  .on('data', (row) => {
    // Check si la ligne n'a pas de cellules vides
    const isRowEmpty = [row.Period, row.Data_value, row.Series_title_2].some((cell) => cell === '');

    // Si la ligne est vide et qu'elle contient les valeurs Credit Debit et Services dans la bonne colonne
    if (!isRowEmpty && ["Credit", "Debit", "Services"].includes(row.Series_title_2)) {
      const filteredRow = {
        id: id++,
        Period: row.Period,
        Data_value: row.Data_value,
        Series_title_2: row.Series_title_2,
      };
      rows.push(filteredRow);
    }
  })
  .on('end', () => {
    csvWriter
      .writeRecords(rows)
      .then(() => console.log('Nettoyage terminé'));
  });
