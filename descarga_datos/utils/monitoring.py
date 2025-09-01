"""
Sistema de monitoreo y métricas para el descargador de datos.
"""
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from datetime import datetime
import json
import logging
from pathlib import Path

@dataclass
class DownloadMetrics:
    """Métricas para una operación de descarga."""
    symbol: str
    exchange: str
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None
    rows_downloaded: int = 0
    retry_count: int = 0
    errors: List[str] = field(default_factory=list)
    data_validation_passed: bool = False
    
    def complete(self, success: bool = True):
        """Marca la operación como completada."""
        self.end_time = time.time()
        return self
    
    @property
    def duration(self) -> float:
        """Duración de la operación en segundos."""
        if self.end_time is None:
            return time.time() - self.start_time
        return self.end_time - self.start_time
    
    def to_dict(self) -> dict:
        """Convierte las métricas a diccionario."""
        return {
            'symbol': self.symbol,
            'exchange': self.exchange,
            'start_time': datetime.fromtimestamp(self.start_time).isoformat(),
            'end_time': datetime.fromtimestamp(self.end_time).isoformat() if self.end_time else None,
            'duration': self.duration,
            'rows_downloaded': self.rows_downloaded,
            'retry_count': self.retry_count,
            'errors': self.errors,
            'data_validation_passed': self.data_validation_passed
        }

class PerformanceMonitor:
    """
    Monitor de rendimiento y métricas del sistema.
    
    Características:
    - Seguimiento de operaciones de descarga
    - Métricas de rendimiento
    - Alertas de rendimiento
    - Persistencia de métricas
    """
    
    def __init__(self, metrics_dir: str = "metrics"):
        self.metrics_dir = Path(metrics_dir)
        self.metrics_dir.mkdir(parents=True, exist_ok=True)
        self.logger = logging.getLogger(__name__)
        self.current_metrics: Dict[str, DownloadMetrics] = {}
        
    def start_operation(self, symbol: str, exchange: str) -> str:
        """Inicia el seguimiento de una operación."""
        timestamp = int(time.time())
        operation_id = f"{exchange}_{symbol}_{timestamp}"
        metrics_dir = self.metrics_dir / exchange / symbol.replace('/', '_')
        metrics_dir.mkdir(parents=True, exist_ok=True)
        self.current_metrics[operation_id] = DownloadMetrics(symbol=symbol, exchange=exchange)
        return operation_id
    
    def update_metrics(self, operation_id: str, **updates):
        """Actualiza las métricas de una operación."""
        if operation_id in self.current_metrics:
            for key, value in updates.items():
                if hasattr(self.current_metrics[operation_id], key):
                    setattr(self.current_metrics[operation_id], key, value)
    
    def complete_operation(self, operation_id: str, success: bool = True):
        """Marca una operación como completada y guarda las métricas."""
        if operation_id in self.current_metrics:
            metrics = self.current_metrics[operation_id].complete(success)
            self._save_metrics(operation_id, metrics)
            self._check_performance_alerts(metrics)
            del self.current_metrics[operation_id]
    
    def _save_metrics(self, operation_id: str, metrics: DownloadMetrics):
        """Guarda las métricas en un archivo JSON."""
        # Crear directorio para el exchange y símbolo
        metrics_subdir = self.metrics_dir / operation_id.split('_')[0] / operation_id.split('_')[1]
        metrics_subdir.mkdir(parents=True, exist_ok=True)
        
        # Guardar métricas
        metrics_file = metrics_subdir / f"{operation_id.split('_')[2]}.json"
        with open(metrics_file, 'w') as f:
            json.dump(metrics.to_dict(), f, indent=2)
    
    def _check_performance_alerts(self, metrics: DownloadMetrics):
        """Verifica si hay problemas de rendimiento que requieran atención."""
        if metrics.duration > 300:  # 5 minutos
            self.logger.warning(
                f"Operación lenta detectada: {metrics.exchange}/{metrics.symbol} "
                f"tomó {metrics.duration:.2f} segundos"
            )
        
        if metrics.retry_count > 5:
            self.logger.warning(
                f"Alto número de reintentos: {metrics.exchange}/{metrics.symbol} "
                f"requirió {metrics.retry_count} reintentos"
            )
        
        if len(metrics.errors) > 0:
            self.logger.error(
                f"Errores durante la descarga de {metrics.exchange}/{metrics.symbol}: "
                f"{', '.join(metrics.errors)}"
            )
    
    def get_performance_summary(self) -> dict:
        """Genera un resumen del rendimiento del sistema."""
        total_operations = 0
        total_errors = 0
        total_rows = 0
        total_duration = 0
        
        for metrics_file in self.metrics_dir.glob("*.json"):
            with open(metrics_file) as f:
                metrics = json.load(f)
                total_operations += 1
                total_errors += len(metrics['errors'])
                total_rows += metrics['rows_downloaded']
                total_duration += metrics['duration']
        
        return {
            'total_operations': total_operations,
            'total_errors': total_errors,
            'total_rows_downloaded': total_rows,
            'average_duration': total_duration / total_operations if total_operations > 0 else 0,
            'success_rate': (total_operations - total_errors) / total_operations if total_operations > 0 else 0
        }
