####################
# 測定局を表示するShinyアプリ
####################
library(shiny)
library(leaflet)
library(mapview)
source(here::here("src/fix_moe_stations_address.R")) # df_moe_stations_location

# Define UI for application that draws a histogram
ui <- fluidPage(

    # Application title
    titlePanel("花粉 測定局"),

    # Sidebar with a slider input for number of bins 
    sidebarLayout(
        sidebarPanel(
        ),

        # Show a plot of the generated distribution
        mainPanel(
            mapview::mapviewOutput("mapPlot")
        )
    )
)

# Define server logic required to draw a histogram
server <- function(input, output) {
    output$mapPlot <- renderMapview({
        m <- 
            df_moe_stations_location %>%
            sf::st_as_sf(coords = c("longitude", "latitude"), crs = 4326) %>%
            mapview::mapview()
    })
}

# Run the application 
shinyApp(ui = ui, server = server)
