import { useEffect, useCallback } from 'react';
import type { ReactNode } from 'react';

interface ChartZoomModalProps {
  isOpen: boolean;
  onClose: () => void;
  title: string;
  children: ReactNode;
}

export function ChartZoomModal({ isOpen, onClose, title, children }: ChartZoomModalProps) {
  const handleKeyDown = useCallback(
    (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        onClose();
      }
    },
    [onClose]
  );

  useEffect(() => {
    if (isOpen) {
      document.addEventListener('keydown', handleKeyDown);
      document.body.style.overflow = 'hidden';
    }
    return () => {
      document.removeEventListener('keydown', handleKeyDown);
      document.body.style.overflow = '';
    };
  }, [isOpen, handleKeyDown]);

  if (!isOpen) return null;

  return (
    <div className="chart-zoom-overlay" onClick={onClose}>
      <div className="chart-zoom-modal" onClick={(e) => e.stopPropagation()}>
        <div className="chart-zoom-header">
          <h2>{title}</h2>
          <button className="chart-zoom-close" onClick={onClose} aria-label="Close">
            <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
              <line x1="18" y1="6" x2="6" y2="18" />
              <line x1="6" y1="6" x2="18" y2="18" />
            </svg>
          </button>
        </div>
        <div className="chart-zoom-content">
          {children}
        </div>
      </div>
    </div>
  );
}
