# Proyecto MinTIC

Proyecto organizado en tres niveles:

- `README.md`: vista general del repositorio.
- `Fase_1/README.md`: documentacion y enlace de la pagina de Fase 1.
- `Fase_2/README.md`: documentacion y enlace de la pagina de Fase 2.

## Enlace de GitHub Pages

https://alejo9812.github.io/XPotencia.ai_Hackaton_MinTIC/

## Acceso rapido

- [README de Fase 1](./Fase_1/README.md)
- [README de Fase 2](./Fase_2/README.md)
- [Pagina de Fase 1](./Fase_1/info/index.html)
- [Pagina de Fase 2](./Fase_2/Info/index.html)

## Abrir en local

Para Fase 1:

```powershell
py -3 -m http.server 4174 --directory Fase_1\info
```

Luego abre:

```text
http://localhost:4174
```

Para Fase 2:

```powershell
py -3 -m http.server 4175 --directory Fase_2\Info
```

Si quieres la version con proxy Dify:

```powershell
$env:DIFY_API_KEY="tu_api_key_de_dify"
node Fase_2\Info\server.mjs
```

Luego abre:

```text
http://localhost:4175
```

## Estructura del proyecto

- `config/`: configuracion y sitio publicado en GitHub Pages
- `Fase_1/`: proyecto independiente de la fase 1
- `Fase_2/`: proyecto independiente de la fase 2

## Presentacion

https://canva.link/j2ld9eka5ljv8hj

## Git para colaboradores

Cada colaborador debe configurar su propio nombre y correo en su equipo para que los commits salgan con su perfil de GitHub:

```bash
git config --global user.name "Tu Nombre"
git config --global user.email "tu-correo-verificado-en-github@ejemplo.com"
```

Si quieres validar que quedó bien:

```bash
git log -1 --format="%an <%ae>"
```
