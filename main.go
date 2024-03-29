package mongoquerymaker

import (
	"fmt"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
)

type pipelineBuilder struct {
	pipeline []bson.M
}

func New() *pipelineBuilder {
	return &pipelineBuilder{
		pipeline: []bson.M{},
	}
}

func (b *pipelineBuilder) findFilterIndex() (bool, int) {
	for idx, pipelineItem := range b.pipeline {
		if _, ok := pipelineItem["$match"]; ok {
			return true, idx
		}
	}

	return false, 0
}

func (b *pipelineBuilder) Filter(filter interface{}) *pipelineBuilder {
	if ok, idx := b.findFilterIndex(); ok {
		b.pipeline[idx]["$match"] = filter
	} else {
		b.pipeline = append(b.pipeline, bson.M{"$match": filter})
	}

	return b
}

func (b *pipelineBuilder) KeywordFilter(keyword string, fields ...string) *pipelineBuilder {
	f := bson.M{"name": bson.M{"$regex": keyword, "$options": "i"}}

	if len(fields) == 1 {
		if ok, idx := b.findFilterIndex(); ok {
			b.pipeline[idx]["$match"].(bson.M)[fields[0]] = f
		} else {
			b.pipeline = append(b.pipeline, bson.M{"$match": bson.M{fields[0]: f}})
		}
	} else {
		items := make([]bson.M, len(fields))
		for idx, field := range fields {
			items[idx] = bson.M{field: f}
		}

		if ok, idx := b.findFilterIndex(); ok {
			if _, ok := b.pipeline[idx]["$match"].(bson.M)["$or"]; ok {
				b.pipeline[idx]["$match"] = bson.M{
					"$and": []bson.M{
						b.pipeline[idx]["$match"].(bson.M),
						{"$or": items},
					},
				}
			} else {
				b.pipeline[idx]["$match"].(bson.M)["$or"] = items
			}
		} else {
			b.pipeline = append(b.pipeline, bson.M{"$match": bson.M{"$or": items}})
		}
	}

	return b
}

type LookupOptions struct {
	Filter    bson.M
	Pipeline  []bson.M
	FromTrash bool
	Exposes   []string
	IsArry    bool
}

func (b *pipelineBuilder) Lookup(from, localField, foreignField, as string, opts *LookupOptions) *pipelineBuilder {
	if opts == nil {
		opts = &LookupOptions{}
	}

	if len(opts.Exposes) == 0 && len(opts.Pipeline) == 0 {
		b.pipeline = append(b.pipeline, bson.M{
			"$lookup": bson.M{
				"from":         from,
				"localField":   localField,
				"foreignField": foreignField,
				"as":           as,
			},
		})

		return b
	}

	condition := bson.A{
		fmt.Sprintf("$%s", foreignField),
		fmt.Sprintf("$$%s_tmp", strings.ReplaceAll(localField, "_", "")),
	}

	filter := []bson.M{}

	if opts.IsArry {
		filter = append(filter, bson.M{
			"$in": condition,
		})
	} else {
		filter = append(filter, bson.M{
			"$eq": condition,
		})
	}

	if len(opts.Filter) > 0 {
		filter = append(filter, opts.Filter)
	}

	if opts.FromTrash {
		filter = append(filter, bson.M{
			"$and": []bson.M{
				{"$ne": bson.A{"$deletedAt", nil}},
				{"$ne": bson.A{bson.M{"$type": "$deletedAt"}, "missing"}},
				{},
			},
		})
	} else {
		filter = append(filter, bson.M{
			"$or": []bson.M{
				{"$eq": bson.A{bson.M{"$type": "$deletedAt"}, "missing"}},
				{"$eq": bson.A{bson.M{"$type": "$deletedAt"}, nil}},
			},
		})
	}

	p := []bson.M{
		{
			"$match": bson.M{
				"$expr": bson.M{"$and": filter},
			},
		},
	}

	p = append(p, opts.Pipeline...)

	if len(opts.Exposes) > 0 {
		project := bson.M{}

		for _, field := range opts.Exposes {
			project[field] = 1
		}

		p = append(p, bson.M{"$project": project})
	}

	b.pipeline = append(b.pipeline, bson.M{
		"$lookup": bson.M{
			"from": from,
			"let": bson.M{
				fmt.Sprintf("%s_tmp", strings.ReplaceAll(localField, "_", "")): fmt.Sprintf("$%s", localField),
			},
			"pipeline": p,
			"as":       as,
		},
	})

	return b
}

func (b *pipelineBuilder) Unwind(field string) *pipelineBuilder {
	b.pipeline = append(b.pipeline, bson.M{"$unwind": bson.M{
		"preserveNullAndEmptyArrays": true,
		"path":                       fmt.Sprintf("$%s", field),
	}})
	return b
}

func (b *pipelineBuilder) Sort(sort interface{}) *pipelineBuilder {
	b.pipeline = append(b.pipeline, bson.M{"$sort": sort})
	return b
}

func (b *pipelineBuilder) Skip(skip int64) *pipelineBuilder {
	if skip > 0 {
		b.pipeline = append(b.pipeline, bson.M{"$skip": skip})
	}

	return b
}

func (b *pipelineBuilder) Limit(limit int64) *pipelineBuilder {
	if limit > 0 {
		b.pipeline = append(b.pipeline, bson.M{"$limit": limit})
	}

	return b
}

func (b *pipelineBuilder) Expose(fields ...string) {
	project := bson.M{}
	for _, field := range fields {
		project[field] = 1
	}

	b.pipeline = append(b.pipeline, bson.M{"$project": project})
}

func (b *pipelineBuilder) Get() []bson.M {
	return b.pipeline
}
