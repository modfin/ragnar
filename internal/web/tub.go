package web

import (
	"context"
	"fmt"
	"net/http"

	"github.com/modfin/ragnar"
	"github.com/modfin/strut"
)

func (web *Web) CreateTub(ctx context.Context, tub ragnar.Tub) strut.Response[ragnar.Tub] {
	requestId := GetRequestID(ctx)

	web.log.Info("Create tub request received")

	restub, err := web.db.CreateTub(ctx, tub)
	if err != nil {
		web.log.Error("error creating tub", "err", err, "request_id", requestId)
		return strut.RespondError[ragnar.Tub](
			http.StatusBadRequest,
			fmt.Sprintf("error creating tub, request_id: %s", requestId),
		)
	}
	// TODO end transaction
	return strut.RespondOk(restub)
}

func (web *Web) ListTubs(ctx context.Context) strut.Response[[]ragnar.Tub] {
	requestId := GetRequestID(ctx)

	tubs, err := web.db.ListTubs(ctx)
	if err != nil {
		web.log.Error("error getting tub list", "err", err, "request_id", requestId)
		return strut.RespondError[string](http.StatusInternalServerError, "err listing tubs, request_id: "+requestId)
	}
	return strut.RespondOk(tubs)
}

func (web *Web) GetTub(ctx context.Context) strut.Response[ragnar.Tub] {
	requestId := GetRequestID(ctx)

	tubname := strut.PathParam(ctx, "tub")
	tub, err := web.db.GetTub(ctx, tubname)
	if err != nil {
		web.log.Error("error getting tub list", "err", err, "request_id", requestId)
		return strut.RespondError[string](http.StatusInternalServerError, "err listing tubs, request_id: "+requestId)
	}
	return strut.RespondOk(tub)
}

func (web *Web) UpdateTub(ctx context.Context, tub ragnar.Tub) strut.Response[ragnar.Tub] {
	requestId := GetRequestID(ctx)

	err := web.db.UpdateTub(ctx, tub)
	if err != nil {
		web.log.Error("error updating tub", "err", err, "request_id", requestId)
		return strut.RespondError[ragnar.Tub](http.StatusBadRequest, fmt.Sprintf("error updating settings: %v", err))
	}

	tub, err = web.db.GetTub(ctx, tub.TubName)
	if err != nil {
		web.log.Error("error getting tub list", "err", err, "request_id", requestId)
		return strut.RespondError[ragnar.Tub](http.StatusInternalServerError, "err getting resulting tub, request_id: "+requestId)
	}

	return strut.RespondOk(tub)
}

func (web *Web) DeleteTub(ctx context.Context) strut.Response[ragnar.Tub] {
	requestId := GetRequestID(ctx)
	web.log.Info("Delete tub request received")
	tubname := strut.PathParam(ctx, "tub")

	tub, err := web.db.GetTub(ctx, tubname)
	if err != nil {
		web.log.Error("error getting tub", "err", err, "request_id", requestId)
		return strut.RespondError[string](http.StatusInternalServerError, "err getting tub, request_id: "+requestId)
	}

	err = web.db.DeleteTub(ctx, tubname)
	if err != nil {
		web.log.Error("error deleting tub", "err", err, "request_id", requestId)
		return strut.RespondError[string](http.StatusInternalServerError, "err deleting tub, request_id: "+requestId)
	}

	err = web.stor.DeleteTub(ctx, tubname)
	if err != nil {
		web.log.Error("error deleting tub from storage", "err", err, "request_id", requestId)
		return strut.RespondError[string](http.StatusInternalServerError, "err deleting tub from storage, request_id: "+requestId)
	}

	return strut.RespondOk(tub)
}
