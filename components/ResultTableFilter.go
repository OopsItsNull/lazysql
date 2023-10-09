package components

import (
	"fmt"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"golang.design/x/clipboard"

	"lazysql/app"
)

type ResultsTableFilter struct {
	*tview.Flex
	Input         *tview.InputField
	Label         *tview.TextView
	currentFilter string
	subscribers   []chan StateChange
	filtering     bool
}

func NewResultsFilter() *ResultsTableFilter {
	recordsFilter := &ResultsTableFilter{
		Flex:  tview.NewFlex(),
		Input: tview.NewInputField(),
		Label: tview.NewTextView(),
	}
	recordsFilter.SetBorder(true)
	recordsFilter.SetDirection(tview.FlexRowCSS)
	recordsFilter.SetTitleAlign(tview.AlignCenter)
	recordsFilter.SetBorderPadding(0, 0, 1, 1)

	recordsFilter.Label.SetTextColor(tcell.ColorOrange)
	recordsFilter.Label.SetText("WHERE")
	recordsFilter.Label.SetBorderPadding(0, 0, 0, 1)

	recordsFilter.Input.SetPlaceholder("Enter a WHERE clause to filter the results")
	recordsFilter.Input.SetPlaceholderStyle(tcell.StyleDefault.Foreground(tcell.ColorWhite).Background(tcell.ColorBlack))
	recordsFilter.Input.SetFieldBackgroundColor(tcell.ColorBlack)
	recordsFilter.Input.SetFieldTextColor(tcell.ColorWhite.TrueColor())
	recordsFilter.Input.SetDoneFunc(func(key tcell.Key) {
		switch key {
		case tcell.KeyEnter:
			if recordsFilter.Input.GetText() != "" {
				recordsFilter.currentFilter = "WHERE " + recordsFilter.Input.GetText()
				recordsFilter.Publish("WHERE " + recordsFilter.Input.GetText())

			}
		case tcell.KeyEscape:
			recordsFilter.currentFilter = ""
			recordsFilter.Input.SetText("")
			recordsFilter.Publish("")

		}
	})
	recordsFilter.Input.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Rune() == 93 {
			fmt.Print("Ctrl+VVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVV")
			bytes := clipboard.Read(clipboard.FmtText)
			recordsFilter.Input.SetText(string(bytes))
			return nil
		}
		return event
	})

	recordsFilter.AddItem(recordsFilter.Label, 6, 0, false)
	recordsFilter.AddItem(recordsFilter.Input, 0, 1, false)

	return recordsFilter
}

func (filter *ResultsTableFilter) Subscribe() chan StateChange {
	subscriber := make(chan StateChange)
	filter.subscribers = append(filter.subscribers, subscriber)
	return subscriber
}

func (filter *ResultsTableFilter) Publish(message string) {
	for _, sub := range filter.subscribers {
		sub <- StateChange{
			Key:   "Filter",
			Value: message,
		}
	}
}

func (filter *ResultsTableFilter) GetIsFiltering() bool {
	return filter.filtering
}

func (filter *ResultsTableFilter) GetCurrentFilter() string {
	return filter.currentFilter
}

func (filter *ResultsTableFilter) SetIsFiltering(filtering bool) {
	filter.filtering = filtering
}

// Function to blur
func (filter *ResultsTableFilter) RemoveHighlight() {
	filter.SetBorderColor(app.BlurTextColor)
	filter.Label.SetTextColor(app.BlurTextColor)
	filter.Input.SetPlaceholderTextColor(app.BlurTextColor)
	filter.Input.SetFieldTextColor(app.BlurTextColor)
}

func (filter *ResultsTableFilter) RemoveLocalHighlight() {
	filter.SetBorderColor(tcell.ColorWhite)
	filter.Label.SetTextColor(tcell.ColorOrange)
	filter.Input.SetPlaceholderTextColor(app.BlurTextColor)
	filter.Input.SetFieldTextColor(app.BlurTextColor)
}

func (filter *ResultsTableFilter) Highlight() {
	filter.SetBorderColor(tcell.ColorWhite)
	filter.Label.SetTextColor(tcell.ColorOrange)
	filter.Input.SetPlaceholderTextColor(tcell.ColorWhite)
	filter.Input.SetFieldTextColor(app.FocusTextColor)
}

func (filter *ResultsTableFilter) HighlightLocal() {
	filter.SetBorderColor(app.FocusTextColor)
	filter.Label.SetTextColor(tcell.ColorOrange)
	filter.Input.SetPlaceholderTextColor(tcell.ColorWhite)
	filter.Input.SetFieldTextColor(app.FocusTextColor)
}