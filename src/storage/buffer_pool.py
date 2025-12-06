from typing import Dict, Optional, Any, Callable
from collections import OrderedDict


class BufferFrame:
    def __init__(self, page_id: str, data: bytes):
        self.page_id = page_id
        self.data = data
        self.dirty = False
        self.pin_count = 0


class BufferPool:
    
    def __init__(self, pool_size: int = 100, page_size: int = 4096):
        self.pool_size = pool_size
        self.page_size = page_size
        self.frames: OrderedDict[str, BufferFrame] = OrderedDict()
        self.hit_count = 0
        self.miss_count = 0
    
    def get_page(self, page_id: str, load_func: Callable[[], bytes]) -> bytes:
        if page_id in self.frames:
            self.hit_count += 1
            self.frames.move_to_end(page_id)
            frame = self.frames[page_id]
            frame.pin_count += 1
            return frame.data
        
        self.miss_count += 1
        
        data = load_func()
        
        if len(self.frames) >= self.pool_size:
            self._evict()
        
        frame = BufferFrame(page_id, data)
        frame.pin_count = 1
        self.frames[page_id] = frame
        
        return data
    
    def put_page(self, page_id: str, data: bytes, mark_dirty: bool = True) -> None:
        if page_id in self.frames:
            frame = self.frames[page_id]
            frame.data = data
            if mark_dirty:
                frame.dirty = True
            self.frames.move_to_end(page_id)
        else:
            if len(self.frames) >= self.pool_size:
                self._evict()
            
            frame = BufferFrame(page_id, data)
            if mark_dirty:
                frame.dirty = True
            self.frames[page_id] = frame
    
    def unpin_page(self, page_id: str) -> None:
        if page_id in self.frames:
            frame = self.frames[page_id]
            if frame.pin_count > 0:
                frame.pin_count -= 1
    
    def flush_page(self, page_id: str, write_func: Callable[[bytes], None]) -> None:
        if page_id in self.frames:
            frame = self.frames[page_id]
            if frame.dirty:
                write_func(frame.data)
                frame.dirty = False
    
    def flush_all(self, write_func_factory: Callable[[str], Callable[[bytes], None]]) -> None:
        for page_id, frame in list(self.frames.items()):
            if frame.dirty:
                write_func = write_func_factory(page_id)
                write_func(frame.data)
                frame.dirty = False
    
    def _evict(self) -> None:
        for page_id, frame in list(self.frames.items()):
            if frame.pin_count == 0:
                del self.frames[page_id]
                return
        
        raise RuntimeError("No unpinned pages available for eviction")
    
    def get_statistics(self) -> Dict[str, Any]:
        total_requests = self.hit_count + self.miss_count
        hit_rate = self.hit_count / total_requests if total_requests > 0 else 0
        
        return {
            "pool_size": self.pool_size,
            "pages_in_buffer": len(self.frames),
            "hit_count": self.hit_count,
            "miss_count": self.miss_count,
            "hit_rate": hit_rate,
            "dirty_pages": sum(1 for f in self.frames.values() if f.dirty)
        }
    
    def clear(self) -> None:
        self.frames.clear()
        self.hit_count = 0
        self.miss_count = 0