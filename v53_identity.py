"""Small private profile decoration, stored as data rather than a remote URL."""

import base64
import binascii
import io

ACCENTS = {"teal", "blue", "violet", "orange"}
PREFIX = "data:image/jpeg;base64,"
_STANDARD_JFIF = ("APP0", b"JFIF\0\x01\x01\0\0\x01\0\x01\0\0")


def identity_spec(value):
    if not isinstance(value, dict) or set(value) - {"name", "note", "accent", "avatar"}:
        raise ValueError("Identità non valida")
    name, note = value.get("name", ""), value.get("note", "")
    avatar, accent = value.get("avatar", ""), value.get("accent", "teal")
    if (
        not isinstance(name, str)
        or len(name) > 60
        or not isinstance(note, str)
        or len(note) > 160
        or accent not in ACCENTS
        or not isinstance(avatar, str)
        or len(avatar) > 48000
    ):
        raise ValueError("Nome, nota o immagine non validi")
    if avatar:
        from PIL import Image, UnidentifiedImageError

        if not avatar.startswith(PREFIX):
            raise ValueError("Immagine profilo non valida")
        try:
            binary = base64.b64decode(avatar[len(PREFIX) :], validate=True)
            with Image.open(io.BytesIO(binary), formats=["JPEG"]) as image:
                if image.size != (128, 128):
                    raise ValueError("Salva l’immagine tramite la scheda Account")
                image.load()
                # Browsers can include an ICC profile even when the original
                # photo is resized on a canvas. Keep just the small image.
                if (
                    image.mode != "RGB"
                    or not binary.endswith(b"\xff\xd9")
                    or image.applist != [_STANDARD_JFIF]
                ):
                    output = io.BytesIO()
                    clean = Image.new("RGB", image.size)
                    clean.paste(image.convert("RGB"))
                    clean.save(output, "JPEG", quality=86, optimize=True)
                    avatar = PREFIX + base64.b64encode(output.getvalue()).decode(
                        "ascii"
                    )
        except (
            binascii.Error,
            UnidentifiedImageError,
            OSError,
            Image.DecompressionBombError,
        ) as exc:
            raise ValueError("Immagine profilo non valida") from exc
    return {
        "name": " ".join(name.split()),
        "note": " ".join(note.split()),
        "accent": accent,
        "avatar": avatar,
    }
