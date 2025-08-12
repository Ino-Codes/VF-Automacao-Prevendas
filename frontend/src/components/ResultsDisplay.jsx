import React from "react";
import * as XLSX from "xlsx"; // Importa a biblioteca para manipulação de Excel

export function ResultsDisplay({ data }) {
  const { com_parcelamento, sem_parcelamento } = data;

  const handleDownload = () => {
    // Cria um novo Workbook
    const wb = XLSX.utils.book_new();

    // Converte os dados JSON para planilhas
    const wsComParcelamento = XLSX.utils.json_to_sheet(com_parcelamento);
    const wsSemParcelamento = XLSX.utils.json_to_sheet(sem_parcelamento);

    // Adiciona as planilhas ao Workbook com nomes específicos
    XLSX.utils.book_append_sheet(wb, wsComParcelamento, "Com Parcelamento");
    XLSX.utils.book_append_sheet(wb, wsSemParcelamento, "Sem Parcelamento");

    // Gera o arquivo Excel e dispara o download
    XLSX.writeFile(wb, "relatorio_analise_pgfn.xlsx");
  };

  const renderTable = (tableData, title) => (
    <div className="table-container">
      <h3>
        {title} ({tableData.length} registros)
      </h3>
      {tableData.length > 0 ? (
        <table>
          <thead>
            <tr>
              {Object.keys(tableData[0]).map((key) => (
                <th key={key}>{key}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {tableData.map((row, index) => (
              <tr key={index}>
                {Object.values(row).map((val, i) => (
                  <td key={i}>{String(val)}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      ) : (
        <p>Nenhum registro encontrado.</p>
      )}
    </div>
  );

  return (
    <div className="results-container">
      <h2>Resultados da Análise</h2>
      <button onClick={handleDownload} className="download-button">
        Baixar Relatório Completo (.xlsx)
      </button>

      <div className="tabs">
        {renderTable(com_parcelamento, "Leads COM Parcelamento Ativo")}
        {renderTable(sem_parcelamento, "Leads SEM Parcelamento Identificado")}
      </div>
    </div>
  );
}
