from rapidfuzz import fuzz

def funcion_fuzz(partnumb_del, campo_descripcion):
    similarity = fuzz.partial_ratio(partnumb_del, campo_descripcion)
    return similarity



