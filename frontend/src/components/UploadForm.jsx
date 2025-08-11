import { useState } from "react";

function UploadForm({ onSubmit }) {
  const [valorMinimo, setValorMinimo] = useState("100000");
  const [file, setFile] = useState(null);
  const [error, setError] = useState("");

  const handleSubmit = (e) => {
    e.preventDefault();
    if (!file || valorMinimo <= 0) {
      setError(
        "Por favor, preencha o valor mínimo e selecione um arquivo Excel."
      );
      return;
    }
    setError("");
    onSubmit({ valorMinimo, file });
  };

  return (
    <form onSubmit={handleSubmit} className="upload-form">
      <div className="form-group">
        <label htmlFor="valorMinimo">Valor Mínimo da Dívida (R$)</label>
        <input
          id="valorMinimo"
          type="number"
          value={valorMinimo}
          onChange={(e) => setValorMinimo(e.target.value)}
          placeholder="Ex: 100000"
        />
      </div>
      <div className="form-group">
        <label htmlFor="file">Arquivo de Parcelamentos (.xlsx)</label>
        <input
          id="file"
          type="file"
          accept=".xlsx"
          onChange={(e) => setFile(e.target.files[0])}
        />
      </div>
      {error && <p className="error-message">{error}</p>}
      <button type="submit">Iniciar Processamento</button>
    </form>
  );
}

export default UploadForm;
