import { Photo } from "../api";

interface Props {
  photos: Photo[];
  onDelete: (key: string) => void;
  deleting: string | null;
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

export function PhotoGrid({ photos, onDelete, deleting }: Props) {
  if (photos.length === 0) {
    return (
      <div className="gallery-empty">
        No photos yet. Upload some above!
      </div>
    );
  }

  return (
    <div className="photo-grid">
      {photos.map((photo) => {
        const filename = photo.key;
        const isDeleting = deleting === photo.key;

        return (
          <div key={photo.key} className={`photo-card ${isDeleting ? "photo-card--deleting" : ""}`}>
            <div className="photo-card__img-wrap">
              {/* The url is a presigned S3 GET URL — the browser fetches the
                  image directly from cloudish without any CORS issue because
                  <img> requests don't trigger preflight checks. */}
              <img
                src={photo.url}
                alt={filename}
                className="photo-card__img"
                loading="lazy"
              />
            </div>
            <div className="photo-card__footer">
              <span className="photo-card__name" title={filename}>
                {filename}
              </span>
              <span className="photo-card__size">{formatBytes(photo.size)}</span>
              <button
                className="photo-card__delete"
                onClick={() => onDelete(photo.key)}
                disabled={isDeleting}
                aria-label={`Delete ${filename}`}
              >
                {isDeleting ? "…" : "✕"}
              </button>
            </div>
          </div>
        );
      })}
    </div>
  );
}
