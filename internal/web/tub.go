package web

import (
	"context"
	"fmt"
	"net/http"

	"github.com/modfin/ragnar"
	"github.com/modfin/strut"
)

func (app *Web) CreateTub(ctx context.Context, tub ragnar.Tub) strut.Response[ragnar.Tub] {
	requestId := GetRequestID(ctx)

	app.log.Info("Create tub request received")

	restub, err := app.db.CreateTub(ctx, tub)
	if err != nil {
		app.log.Error("error creating tub", "err", err, "request_id", requestId)
		return strut.RespondError[ragnar.Tub](
			http.StatusBadRequest,
			fmt.Sprintf("error creating tub, request_id: %s", requestId),
		)
	}
	// TODO end transaction
	return strut.RespondOk(restub)
}

func (app *Web) ListTubs(ctx context.Context) strut.Response[[]ragnar.Tub] {
	requestId := GetRequestID(ctx)

	tubs, err := app.db.ListTubs(ctx)
	if err != nil {
		app.log.Error("error getting tub list", "err", err, "request_id", requestId)
		return strut.RespondError[string](http.StatusInternalServerError, "err listing tubs, request_id: "+requestId)
	}
	return strut.RespondOk(tubs)
}

func (app *Web) GetTub(ctx context.Context) strut.Response[ragnar.Tub] {
	requestId := GetRequestID(ctx)

	tubname := strut.PathParam(ctx, "tub")
	tub, err := app.db.GetTub(ctx, tubname)
	if err != nil {
		app.log.Error("error getting tub list", "err", err, "request_id", requestId)
		return strut.RespondError[string](http.StatusInternalServerError, "err listing tubs, request_id: "+requestId)
	}
	return strut.RespondOk(tub)
}

func (app *Web) UpdateTub(ctx context.Context, tub ragnar.Tub) strut.Response[ragnar.Tub] {
	requestId := GetRequestID(ctx)

	err := app.db.UpdateTub(ctx, tub)
	if err != nil {
		app.log.Error("error updating tub", "err", err, "request_id", requestId)
		return strut.RespondError[ragnar.Tub](http.StatusBadRequest, fmt.Sprintf("error updating settings: %v", err))
	}

	tub, err = app.db.GetTub(ctx, tub.TubName)
	if err != nil {
		app.log.Error("error getting tub list", "err", err, "request_id", requestId)
		return strut.RespondError[ragnar.Tub](http.StatusInternalServerError, "err getting resulting tub, request_id: "+requestId)
	}

	return strut.RespondOk(tub)
}

func (app *Web) DeleteTub(ctx context.Context) strut.Response[ragnar.Tub] {
	requestId := GetRequestID(ctx)
	app.log.Info("Delete tub request received")
	tubname := strut.PathParam(ctx, "tub")

	tub, err := app.db.GetTub(ctx, tubname)
	if err != nil {
		app.log.Error("error getting tub", "err", err, "request_id", requestId)
		return strut.RespondError[string](http.StatusInternalServerError, "err getting tub, request_id: "+requestId)
	}

	err = app.db.DeleteTub(ctx, tubname)
	if err != nil {
		app.log.Error("error deleting tub", "err", err, "request_id", requestId)
		return strut.RespondError[string](http.StatusInternalServerError, "err deleting tub, request_id: "+requestId)
	}

	err = app.stor.DeleteTub(ctx, tubname)
	if err != nil {
		app.log.Error("error deleting tub from storage", "err", err, "request_id", requestId)
		return strut.RespondError[string](http.StatusInternalServerError, "err deleting tub from storage, request_id: "+requestId)
	}

	return strut.RespondOk(tub)
}
