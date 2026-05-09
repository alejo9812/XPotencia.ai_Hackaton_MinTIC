# Proyecto MinTIC

Proyecto organizado en cuatro capas dentro de `Fase_2/`, con la raiz de esa carpeta limpia para que no aparezca nada fuera del flujo.

## Enlace de GitHub Pages

https://alejo9812.github.io/XPotencia.ai_Hackaton_MinTIC/

## Acceso rapido

- [README de Fase 1](./Fase_1/README.md)
- [Capa 4 de Fase 2](./Fase_2/capa_4_score_riesgo_explicable/README.md)
- [Capa 1 de Fase 2](./Fase_2/capa_1_ingesta_automatizada/README.md)
- [Capa 2 de Fase 2](./Fase_2/capa_2_motor_reglas_cuantitativas/README.md)
- [Capa 3 de Fase 2](./Fase_2/capa_3_analisis_semantico_llm/README.md)
- [Roadmap Fase 2](./Fase_2/capa_4_score_riesgo_explicable/docs/ROADMAP_FASE_2.md)
- [Interface Spec Fase 2](./Fase_2/capa_4_score_riesgo_explicable/docs/INTERFACE_SPEC.md)
- [Agent Integration Guide Fase 2](./Fase_2/capa_4_score_riesgo_explicable/docs/AGENT_INTEGRATION_GUIDE.md)
- [Arquitectura por capas de Fase 2](./Fase_2/capa_4_score_riesgo_explicable/README.md)
- [Pagina de Fase 1](./Fase_1/info/index.html)
- [Interfaz principal de Fase 2](./Fase_2/capa_4_score_riesgo_explicable/Info/index.html)
- [Dashboard PAE de Fase 2](./Fase_2/capa_4_score_riesgo_explicable/Info/dashboard-opacidad-pae.html)
- [PAE Risk Tracker](./Fase_2/capa_1_ingesta_automatizada/pae_risk_tracker/README.md)

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

Luego abre:

```text
http://localhost:4175
```

Para el tracker PAE:

```powershell
py -3 Fase_2\pae_risk_tracker\scripts\discover_schema.py
py -3 Fase_2\pae_risk_tracker\scripts\download_pae_sample.py
```

Si quieres la interfaz principal de Fase 2 conectada a la API PAE:

```powershell
$env:PAE_CHAT_BACKEND_URL="http://127.0.0.1:8000"
node Fase_2\Info\server.mjs
```

Luego abre:

```text
http://localhost:4175
```

La vista clasica sigue disponible en:

```text
http://localhost:4175/dashboard-opacidad-pae.html
```

## Estructura del proyecto

- `config/`: configuracion y sitio publicado en GitHub Pages
- `Fase_1/`: proyecto independiente de la fase 1
- `Fase_2/`: proyecto independiente de la fase 2
- `Fase_2/capa_1_ingesta_automatizada/`, `Fase_2/capa_2_motor_reglas_cuantitativas/`, `Fase_2/capa_3_analisis_semantico_llm/`, `Fase_2/capa_4_score_riesgo_explicable/`: documentacion por capas con README y PDF por carpeta

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
