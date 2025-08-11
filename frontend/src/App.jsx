import { useState, useEffect } from "react";
import axios from "axios";
import UploadForm from "./components/UploadForm.jsx";
import ResultsDisplay from "./components/ResultsDisplay.jsx";
import "./App.css";

console.log("VITE_API_URL DENTRO DO CÓDIGO:", import.meta.env.VITE_API_URL);

// Garanta que esta é a linha 8 (ou a linha que define API_URL)
const API_URL = import.meta.env.VITE_API_URL;

function App() {
  const [jobId, setJobId] = useState(null);
  const [jobStatus, setJobStatus] = useState("idle"); // idle, processing, completed, error
  const [results, setResults] = useState(null);
  const [error, setError] = useState("");

  // Efeito para consultar o status do job periodicamente
  useEffect(() => {
    let intervalId;
    if (jobId && jobStatus === "processing") {
      intervalId = setInterval(async () => {
        try {
          const response = await axios.get(`${API_URL}/status/${jobId}`);
          const { status } = response.data;
          if (status === "concluido") {
            setJobStatus("completed");
            const resultResponse = await axios.get(
              `${API_URL}/resultado/${jobId}`
            );
            setResults(resultResponse.data.resultado);
            clearInterval(intervalId);
          } else if (status === "erro") {
            setJobStatus("error");
            setError("Ocorreu um erro no processamento no servidor.");
            clearInterval(intervalId);
          }
        } catch (err) {
          console.error("Erro ao verificar status:", err);
          setError(
            "Não foi possível comunicar com o servidor para verificar o status."
          );
          setJobStatus("error");
          clearInterval(intervalId);
        }
      }, 5000); // Consulta a cada 5 segundos
    }
    // Limpa o intervalo quando o componente é desmontado ou o job termina
    return () => clearInterval(intervalId);
  }, [jobId, jobStatus]);

  const handleProcessRequest = async ({ valorMinimo, file }) => {
    const formData = new FormData();
    formData.append("valor_minimo", valorMinimo);
    formData.append("file", file);

    setJobStatus("processing");
    setError("");
    setResults(null);
    setJobId(null);

    try {
      const response = await axios.post(`${API_URL}/processar`, formData, {
        headers: { "Content-Type": "multipart/form-data" },
      });
      setJobId(response.data.job_id);
    } catch (err) {
      console.error("Erro ao iniciar processamento:", err);
      setError(
        "Falha ao enviar a requisição de processamento. Verifique o console."
      );
      setJobStatus("error");
    }
  };

  const handleReset = () => {
    setJobId(null);
    setJobStatus("idle");
    setResults(null);
    setError("");
  };

  return (
    <div className="container">
      <header>
        <h1>Análise de Devedores PGFN</h1>
        <p>
          Faça o upload dos dados para iniciar a análise e cruzamento de
          informações.
        </p>
      </header>

      {jobStatus === "idle" && <UploadForm onSubmit={handleProcessRequest} />}

      {jobStatus === "processing" && (
        <div className="status-box">
          <h2>Processando...</h2>
          <p>
            Sua solicitação está sendo processada no servidor. Por favor,
            aguarde.
          </p>
          <div className="loader"></div>
        </div>
      )}

      {jobStatus === "error" && (
        <div className="status-box error">
          <h2>Erro</h2>
          <p>{error}</p>
          <button onClick={handleReset}>Tentar Novamente</button>
        </div>
      )}

      {jobStatus === "completed" && results && (
        <div>
          <ResultsDisplay data={results} />
          <button onClick={handleReset} className="reset-button">
            Fazer Nova Análise
          </button>
        </div>
      )}
    </div>
  );
}

export default App;
