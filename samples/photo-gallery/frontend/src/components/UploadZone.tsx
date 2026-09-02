import { useRef, useState, DragEvent, ChangeEvent } from "react";

interface Props {
  onUpload: (files: File[]) => void;
  uploading: boolean;
}

export function UploadZone({ onUpload, uploading }: Props) {
  const [dragging, setDragging] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);

  function handleDrop(e: DragEvent<HTMLDivElement>) {
    e.preventDefault();
    setDragging(false);
    const files = Array.from(e.dataTransfer.files).filter((f) =>
      f.type.startsWith("image/")
    );
    if (files.length) onUpload(files);
  }

  function handleChange(e: ChangeEvent<HTMLInputElement>) {
    const files = Array.from(e.target.files ?? []);
    if (files.length) onUpload(files);
    // Reset so the same file can be re-selected after deletion.
    e.target.value = "";
  }

  return (
    <div
      className={`upload-zone ${dragging ? "upload-zone--active" : ""} ${uploading ? "upload-zone--busy" : ""}`}
      onClick={() => !uploading && inputRef.current?.click()}
      onDragOver={(e) => { e.preventDefault(); setDragging(true); }}
      onDragLeave={() => setDragging(false)}
      onDrop={handleDrop}
      role="button"
      aria-label="Upload photos"
    >
      <input
        ref={inputRef}
        type="file"
        accept="image/*"
        multiple
        className="upload-zone__input"
        onChange={handleChange}
        disabled={uploading}
      />
      {uploading ? (
        <span>Uploading…</span>
      ) : (
        <span>
          <strong>Click</strong> or drag &amp; drop images here
        </span>
      )}
    </div>
  );
}
