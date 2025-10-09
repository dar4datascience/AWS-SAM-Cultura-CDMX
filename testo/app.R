library(shiny)
library(reactable)
library(bslib)
library(shiny)
library(reactable)
library(dplyr)

library(arrow)
library(dplyr)
library(janitor)
library(R.utils) # for gunzip
library(reactable)
library(htmltools)
library(fontawesome)

# Raw URL of gzip-compressed parquet on GitHub
raw_gz_url <- "https://raw.githubusercontent.com/dar4datascience/AWS-SAM-Cultura-CDMX/main/data/scraped_data_cultura_cartelera_cdmx.parquet"

# Download gzip file
tmp_gz <- tempfile(fileext = ".parquet.gz")
download.file(raw_gz_url, tmp_gz, mode = "wb")

# Decompress gzip to Parquet
tmp_parquet <- tempfile(fileext = ".parquet")
R.utils::gunzip(tmp_gz, destname = tmp_parquet, overwrite = TRUE)

# Read Parquet into a tibble
cartelera_cultural_cdmx <- arrow::read_parquet(tmp_parquet) |>
  janitor::clean_names()

ui <- fluidPage(
  titlePanel("row selection example"),
  reactableOutput("table"),
  verbatimTextOutput("selected")
)

server <- function(input, output, session) {
  selected <- reactive(getReactableState("table", "selected"))

  output$table <- renderReactable({
    reactable(cartelera_cultural_cdmx, selection = "multiple", onClick = "select")
  })

  output$selected <- renderPrint({
    print(selected())
  })

  observe({
    print(cartelera_cultural_cdmx[selected(), ])
  })
}

shinyApp(ui, server)
