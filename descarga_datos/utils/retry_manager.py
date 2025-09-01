"""
Módulo para gestionar reintentos de operaciones con backoff exponencial.
"""
import time
import random
import asyncio
from functools import wraps
from typing import Callable, Any, Optional, Type, Union
import logging

class RetryError(Exception):
    """Error cuando se agotan los reintentos."""
    pass

class RetryManager:
    """
    Gestiona reintentos de operaciones con backoff exponencial.
    
    Características:
    - Backoff exponencial con jitter
    - Manejo específico por tipo de error
    - Logging detallado de reintentos
    - Límites configurables
    """
    
    def __init__(
        self,
        max_retries: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        exponential_base: float = 2,
        jitter: bool = True
    ):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base
        self.jitter = jitter
        self.logger = logging.getLogger(__name__)
        
        # Errores específicos y sus estrategias
        self.error_handlers = {
            'RateLimitExceeded': self._handle_rate_limit,
            'NetworkError': self._handle_network_error,
            'TimeoutError': self._handle_timeout
        }
    
    def calculate_delay(self, attempt: int) -> float:
        """Calcula el tiempo de espera para el siguiente reintento."""
        delay = min(
            self.base_delay * (self.exponential_base ** attempt),
            self.max_delay
        )
        
        if self.jitter:
            delay *= (0.5 + random.random())
            
        return delay
    
    def _handle_rate_limit(self, error: Exception) -> float:
        """Maneja errores de límite de tasa."""
        # Aquí podríamos extraer el tiempo de espera de la respuesta de la API
        return self.max_delay
    
    def _handle_network_error(self, error: Exception) -> float:
        """Maneja errores de red."""
        return self.calculate_delay(1)
    
    def _handle_timeout(self, error: Exception) -> float:
        """Maneja errores de timeout."""
        return self.calculate_delay(2)
    
    def get_error_wait_time(self, error: Exception) -> float:
        """Determina el tiempo de espera basado en el tipo de error."""
        error_type = error.__class__.__name__
        handler = self.error_handlers.get(error_type)
        
        if handler:
            return handler(error)
        return self.base_delay
    
    async def execute_with_retry(
        self,
        func: Callable,
        *args,
        **kwargs
    ) -> Any:
        """
        Ejecuta una función con reintentos automáticos.
        
        Args:
            func: Función a ejecutar
            *args: Argumentos posicionales
            **kwargs: Argumentos nombrados
            
        Returns:
            El resultado de la función
            
        Raises:
            RetryError: Si se agotan los reintentos
        """
        last_error = None
        
        for attempt in range(self.max_retries):
            try:
                if asyncio.iscoroutinefunction(func):
                    result = await func(*args, **kwargs)
                else:
                    result = func(*args, **kwargs)
                return result
                
            except Exception as e:
                last_error = e
                wait_time = self.get_error_wait_time(e)
                
                self.logger.warning(
                    f"Intento {attempt + 1}/{self.max_retries} falló: {str(e)}. "
                    f"Esperando {wait_time:.2f}s antes del siguiente intento."
                )
                
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(wait_time)
                    
        raise RetryError(f"Máximo de reintentos ({self.max_retries}) alcanzado. Último error: {str(last_error)}")

def with_retry(
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2,
    jitter: bool = True
):
    """
    Decorador para aplicar reintentos a una función.
    
    Ejemplo:
        @with_retry(max_retries=5, base_delay=2.0)
        async def fetch_data():
            ...
    """
    retry_manager = RetryManager(
        max_retries=max_retries,
        base_delay=base_delay,
        max_delay=max_delay,
        exponential_base=exponential_base,
        jitter=jitter
    )
    
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs):
            return await retry_manager.execute_with_retry(func, *args, **kwargs)
        return wrapper
    
    return decorator
